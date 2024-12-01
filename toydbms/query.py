"""Abstract representation of a query."""


import typing as t
from abc import ABC, abstractmethod
from dataclasses import dataclass

from toydbms.physical import DType


@dataclass
class Table:
    """Represents an on-disk table for an abstract query."""

    schema: t.List[t.Tuple[str, t.Type[DType]]]
    data_path: str

    @property
    def columns(self) -> t.List[str]:
        return [c for c, _ in self.schema]


@dataclass
class Filter:
    """Represents a selection filter for an abstract query.

    column_args: A list of column names, in the order they are expected as args to the predicate function.
    predicate: A predicate function to evaluate on each row.
    """

    column_args: t.List[str]
    predicate: t.Callable[..., bool]


@dataclass
class SortColumn:
    """Represents a sorting column for an abstract query."""

    column: str
    asc: bool = True


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


@dataclass
class AbstractQuery:
    from_clause: Table
    select_clause: t.Optional[t.List[str]] = None
    where_clause: t.Optional[Filter] = None
    order_clause: t.Optional[t.List[SortColumn]] = None
    limit_clause: t.Optional[int] = None