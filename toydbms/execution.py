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
        self._child = child

    @property
    def table(self) -> Table:
        """Allows a chain or iterators to all access the input table in case schema is needed."""
        return self._child.table

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
        self._child = self
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


class ValuesNode(Node):
    """Represents a list of values in a query as a node."""
    def __init__(self, table: Table, values: t.List[t.List[str]]):
        self._table = table
        self._child = self
        self._values = values
        self._idx = 0
    
    def __iter__(self):
        return self

    def __next__(self) -> t.List[t.Any]:
        try:
            record = [
                dtype.from_str(val)
                for (_, dtype), val in zip(self._table.schema, self._values[self._idx])
            ]
            self._idx += 1
            return record
        except IndexError:
            raise StopIteration


class InsertNode(Node):
    """Inserts to the end of a table heap file."""
    def __init__(
            self,
            child: Node,
            table: Table,
            page_size: int = DEFAULT_PAGE_SIZE
        ):
        self._dest_table = table
        self._child = child
        self._page_size = page_size
        self._num_inserted = 0

    @property
    def table(self) -> Table:
        return self._dest_table
    
    def __iter__(self):
        return self
    
    def __next__(self) -> t.List[t.Any]:
        """For now, process all inserts in one batch and return num inserted."""
        if self._num_inserted > 0:
            raise StopIteration
        with open(self._dest_table.data_path, 'rb+') as f:
            # initialize with last page in the table
            f.seek(-self._page_size, SEEK_END)
            page = HeapPage(self._dest_table.schema, init_buff=f.read(self._page_size))
            f.seek(-self._page_size, SEEK_END)
            for record in self._child:
                try:
                    page.insert_record(record)
                except InsufficientSpaceError:
                    f.write(page.marshall())
                    page = HeapPage(self._dest_table.schema)
                    page.insert_record(record)
                self._num_inserted += 1
            if page.num_records > 0:
                f.write(page.marshall())
            return [self._num_inserted]


class ProjectionNode(Node):
    def __init__(
        self,
        child: Node,
        projection_columns: t.List[str],
    ):
        self._child = child
        self.projection_col_idxs = [
            self.table.columns.index(col) for col in projection_columns
        ]

    def __iter__(self):
        return self

    def __next__(self) -> t.List[t.Any]:
        row = next(self._child)
        return [row[i] for i in self.projection_col_idxs]


class SelectionNode(Node):
    def __init__(self, child: Node, filter: Filter):
        self._child = child
        self.filter = filter
        self.col_arg_idxs = [
            self.table.columns.index(col) for col in filter.column_args
        ]

    def __iter__(self):
        return self

    def __next__(self) -> t.List[t.Any]:
        while True:
            row = next(self._child)
            if self.filter.predicate(*[row[i] for i in self.col_arg_idxs]):
                return row


class LimitNode(Node):
    def __init__(self, child: Node, limit: int):
        self._child = child
        self.limit = limit

    def __iter__(self):
        return self

    def __next__(self) -> t.List[t.Any]:
        if self.limit <= 0:
            raise StopIteration
        self.limit -= 1
        return next(self._child)


class SortNode(Node):
    def __init__(self, child: Node, sort_columns: t.List[SortColumn]):
        self._child = child
        self.sort_columns = sort_columns
        # Store reverse sorted rows so they can be popped when node is iterated
        self.reverse_sorted_rows = None

    def _init_sorted_rows(self) -> None:
        rows = [r for r in self._child]
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
    

def _get_abstract_query_entry_node(s: AbstractQuery) -> Node:
    entry_node = FileScanNode(s.from_clause)
    if s.where_clause is not None:
        entry_node = SelectionNode(entry_node, s.where_clause)
    if s.order_clause:
        entry_node = SortNode(entry_node, s.order_clause)
    if s.limit_clause:
        entry_node = LimitNode(entry_node, s.limit_clause)
    if s.select_clause:
        entry_node = ProjectionNode(entry_node, s.select_clause)
    return entry_node


def execute_dml(s: AbstractDMLStatement) -> t.List[t.List[t.Any]]:
    if isinstance(s, AbstractQuery):
        entry_node = _get_abstract_query_entry_node(s)
    elif isinstance(s, AbstractInsert):
        # self._child is an interator yielding t.List[Any] with the same schema
        # as the input table
        if s.values_clause is not None and s.from_clause is not None:
            raise ValueError("Can't define both values_clause and from_clause")
        elif s.values_clause is not None:
            child = ValuesNode(s.into_clause, s.values_clause)
        elif s.from_clause is not None:
            child = _get_abstract_query_entry_node(s.from_clause)
        else:
            raise ValueError("One of values_clause or from_clause must be defined")
        entry_node = InsertNode(child, s.into_clause)
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