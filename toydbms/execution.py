import os
import typing as t
from abc import ABC, abstractmethod
from io import BytesIO, SEEK_END

from toydbms.physical import HeapPage, InsufficientSpaceError
from toydbms.query import (
    AbstractCreateTable,
    AbstractDDLStatement,
    AbstractDMLStatement,
    AbstractInsert,
    AbstractStatement,
    AbstractQuery,
    Filter,
    SortColumn,
    Table
)


DEFAULT_PAGE_SIZE = 4096


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
    """Scans an on-disk database table."""

    def __init__(self, table: Table, page_size: int = DEFAULT_PAGE_SIZE):
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

    def __next__(self) -> t.List[t.Any]:
        while True:
            if self._page is None:
                page_bin = self._file.read(self._page_size)
                if len(page_bin) == 0:
                    raise StopIteration
                elif len(page_bin) != self._page_size:
                    raise ValueError("heapfile size isn't multiple of page size") 
                self._page = HeapPage(self._input_table.schema, init_buff=page_bin)
            try:
                return next(self._page)
            except StopIteration:
                self._page = None
                continue

    def __del__(self):
        self._file.close()


class InsertNode(Node):
    """Inserts to the end of a table heap file.
    
    TODO: rework as iterator taking values or query result
    """

    def __init__(self, table: Table, values: t.List[t.List[str]], page_size: int = DEFAULT_PAGE_SIZE):
        self._values_written = False
        self._input_table = table
        self._values = values
        self._page_size = page_size
        self.child = self

    @property
    def table(self) -> Table:
        return self._input_table
    
    def __iter__(self):
        return self
    
    # for now, just insert in one batch and return number of records inserted
    # until I figure out if/how inserts should fit into the iteration model
    def __next__(self) -> t.List[t.Any]:
        if self._values_written:
            raise StopIteration
        with open(self._input_table.data_path, 'rb+') as f:
            # Initialize with last page of file. For now, assumes at least one page
            # exists
            f.seek(-self._page_size, SEEK_END)
            page = HeapPage(self._input_table.schema, init_buff=f.read(self._page_size))
            # Maintain an invariant that the file location is always the start
            # of the currently loaded page
            f.seek(-self._page_size, SEEK_END)
            for record in self._values:
                try:
                    page.insert_record(record)
                except InsufficientSpaceError:
                    print('here')
                    f.write(page.marshall())
                    page = HeapPage(self._input_table.schema)
                    page.insert_record(record)
            if page.num_records > 0:
                f.write(page.marshall())
            self._values_written = True
            return [len(self._values)]


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

    def __next__(self) -> t.List[t.Any]:
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

    def __next__(self) -> t.List[t.Any]:
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

    def __next__(self) -> t.List[t.Any]:
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

    def __next__(self) -> t.List[t.Any]:
        if self.reverse_sorted_rows is None:
            self._init_sorted_rows()
        if not self.reverse_sorted_rows:
            raise StopIteration
        return self.reverse_sorted_rows.pop()
    

def execute_ddl(s: AbstractDDLStatement) -> None:
    if isinstance(s, AbstractCreateTable):
        # Write empty page for new table
        # TODO: need some sort of catalog to track, for now just throw if file
        # is already at the specified path
        if os.path.exists(s.table.data_path):
            raise ValueError(f"Table {s.table.data_path} already exists")
        with open(s.table.data_path, 'wb') as fo:
            page = HeapPage(s.table.schema)
            fo.write(page.marshall())
    else:
        raise ValueError(f"Received unrecognized AbstractDDLStatement type: {type(s)}")
    

def execute_dml(s: AbstractDMLStatement) -> t.List[t.List[t.Any]]:
    if isinstance(s, AbstractQuery):
        entry_node = FileScanNode(s.from_clause)
        if s.where_clause is not None:
            entry_node = SelectionNode(entry_node, s.where_clause)
        if s.order_clause:
            entry_node = SortNode(entry_node, s.order_clause)
        if s.limit_clause:
            entry_node = LimitNode(entry_node, s.limit_clause)
        if s.select_clause:
            entry_node = ProjectionNode(entry_node, s.select_clause)
    elif isinstance(s, AbstractInsert):
        entry_node = InsertNode(s.into_clause, s.values_clause)
    else:
        raise ValueError(f"Received unrecognized AbstractDMLStatement type: {type(s)}")
    return [r for r in entry_node]
    

def execute(s: AbstractStatement) -> t.Optional[t.List[t.List[t.Any]]]:
    if isinstance(s, AbstractDDLStatement):
       return execute_ddl(s)
    elif isinstance(s, AbstractDMLStatement):
        return execute_dml(s)
    else:
        raise ValueError(f"Received unrecognized AbstractStatement type: {type(s)}")