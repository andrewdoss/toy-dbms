import typing as t
from abc import ABC, abstractmethod
from io import BytesIO

from toydbms.physical import HeapPage
from toydbms.query import AbstractQuery, Filter, SortColumn, Table


class Node(ABC):
    """The base Node interface is equivalent to the iterator interface."""

    def __init__(self, child: "Node"):
        self.child = child

    @property
    def table(self) -> Table:
        """Allows a chain or iterators to all access the input table in case schema is needed."""
        return self.child.table

    def __iter__(self) -> "Node":
        """Each Node instance is both iterator and iterable, so it can only be iterated once."""
        return self

    @abstractmethod
    def __next__(self) -> t.List[t.Any]:
        pass


class FileScanNode(Node):
    """Simulates a table scan using an on-disk database table."""

    def __init__(self, table: Table, page_size: int = 4096):
        self._input_table = table
        self._page_size = page_size
        self._file = open(table.data_path, 'rb')
        self.child = self
        self._page = None

    @property
    def table(self) -> Table:
        return self._input_table

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            if self._page is None:
                page_bin = self._file.read(self._page_size)
                if len(page_bin) == 0:
                    raise StopIteration
                elif len(page_bin) != self._page_size:
                    raise ValueError("heapfile size isn't multiple of page size") 
                self._page = HeapPage(init_buff=page_bin)
            try:
                record_buff = BytesIO(next(self._page))
                return [
                    dtype.unmarshall(record_buff)
                    for (_, dtype) in self._input_table.schema
                ]
            except StopIteration:
                self._page = None
                continue


    def __del__(self):
        self._file.close()


class ProjectionNode(Node):
    def __init__(
        self,
        child: Node,
        projection_columns: t.List[str],
    ):
        self.child = child
        self.projection_col_idxs = [
            self.table.columns.index(col) for col in projection_columns
        ]

    def __iter__(self):
        return self

    def __next__(self):
        row = next(self.child)
        return [row[i] for i in self.projection_col_idxs]


class SelectionNode(Node):
    def __init__(self, child: Node, filter: Filter):
        self.child = child
        self.filter = filter
        self.col_arg_idxs = [
            self.table.columns.index(col) for col in filter.column_args
        ]

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            row = next(self.child)
            if self.filter.predicate(*[row[i] for i in self.col_arg_idxs]):
                return row


class LimitNode(Node):
    def __init__(self, child: Node, limit: int):
        self.child = child
        self.limit = limit

    def __iter__(self):
        return self

    def __next__(self):
        if self.limit <= 0:
            raise StopIteration
        self.limit -= 1
        return next(self.child)


class SortNode(Node):
    def __init__(self, child: Node, sort_columns: t.List[SortColumn]):
        self.child = child
        self.sort_columns = sort_columns
        # Store reverse sorted rows so they can be popped when node is iterated
        self.reverse_sorted_rows = None

    def _init_sorted_rows(self) -> None:
        rows = [r for r in self.child]
        # sort in opposite order of specifed columns for correct pecedence
        for sort_column in reversed(self.sort_columns):
            col_idx = self.table.columns.index(sort_column.column)
            # reverse sort order so iteration can pop from tail
            rows.sort(key=lambda r: r[col_idx], reverse=sort_column.asc)
        self.reverse_sorted_rows = rows

    def __iter__(self):
        return self

    def __next__(self):
        if self.reverse_sorted_rows is None:
            self._init_sorted_rows()
        if not self.reverse_sorted_rows:
            raise StopIteration
        return self.reverse_sorted_rows.pop()
    

def execute(q: AbstractQuery) -> t.List[t.List[t.Any]]:
    entry_node = FileScanNode(q.from_clause)
    if q.where_clause is not None:
        entry_node = SelectionNode(entry_node, q.where_clause)
    if q.order_clause:
        entry_node = SortNode(entry_node, q.order_clause)
    if q.limit_clause:
        entry_node = LimitNode(entry_node, q.limit_clause)
    if q.select_clause:
        entry_node = ProjectionNode(entry_node, q.select_clause)
    return [r for r in entry_node]