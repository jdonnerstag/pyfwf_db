#!/usr/bin/env python
# encoding: utf-8

"""Base classes for indexes"""

import abc
import sys
import collections.abc
from typing import Iterable, Iterator, Any, TypeVar, Generic
from prettytable import PrettyTable

from .fwf_dict import FWFDict
from .fwf_view_like import FWFViewLike
from .fwf_subset import FWFSubset
from .fwf_line import FWFLine


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

class FWFIndexBuilder:
    """An Mixin defining some common core functionalities to build an index."""

    def index(self, parent: FWFViewLike, field: int|str, **kwargs):
        """A convience function to create the index without generator"""

        field = parent.field_from_index(field)

        # Create an iterator which iterates over all relevant records
        gen = self.index_generator(parent, field, **kwargs)

        # Do we need to apply any transformations...
        func = kwargs.get("func", None)
        if func is not None and callable(func):
            gen = (func(v) for v in gen)

        # Print some log-progress if requested, possible with every line
        log_progress = kwargs.get("log_progress", None)
        if log_progress is not None and callable(log_progress):
            gen = (log_progress(parent, i) or v for i, v in enumerate(gen))

        # Consume the iterator and create the index
        self.create_index_from_generator(parent, gen, **kwargs)

        return self


    def index_generator(self, parent: FWFViewLike, field: int|str, **_) -> Iterator[memoryview]:
        '''Provide an iterator (e.g generator) which iterates over all relevant records'''
        return parent.iter_lines_with_field(field)


    def create_index_from_generator(self, parent: FWFViewLike, gen: Iterator[memoryview], **kwargs) -> None:
        """Consume the iterator or generator and create the index"""


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

# 'T' will be either FWFLine or FWFSubset
T = TypeVar('T')

class FWFIndexLike(Generic[T], collections.abc.Mapping[Any, T]):
    """An abstract base class defining the minimum methods and
    required core functionalities of every index like class.
    """

    def __init__(self, parent: FWFViewLike, data: dict):
        self.parent = parent
        self.data: dict = data


    def keys(self) -> Iterable[Any]:
        """Return an iterable sequence of the index keys"""
        return self.data.keys()


    def __iter__(self) -> Iterator[tuple[Any, T]]:
        """Return an iterable sequence of the index keys"""
        yield from self.items()


    def __len__(self) -> int:
        return self.count()


    def count(self) -> int:
        """Return the number of keys in this index"""
        return len(self.data)


    def __getitem__(self, key) -> T:
        """Create a new view with all rows matching the index key.

        Throwing a KeyError if key is not found
        """

        return self.to_T(self.data[key])


    def get(self, key, default=None) -> None | T:
        """Create a new view with all rows matching the index key"""
        try:
            return self[key]
        except KeyError:
            return default


    @abc.abstractmethod
    def to_T(self, value: int|list[int]) -> T:      # pylint: disable=invalid-name
        """Convert an int to a FWFLine, and [int] to FWFSubset"""


    def items(self) -> Iterator[tuple[Any, T]]:
        """Iterate over all key/value tuples"""

        for key, value in self.data.items():
            yield key, self.to_T(value)


    def __contains__(self, param: Any) -> bool:
        return param in self.data


    def __setitem__(self, key, value: int|list[int]) -> None:
        self.data[key] = value


    # pylint: disable=unused-argument
    def get_string(self, *fields: str, stop: int = 10, pretty: bool = True) -> str:
        """Get a string represention of the index"""

        rtn = f"{self.__class__.__name__}(count={self.count()}): ["

        stop = self.count() if stop < 0 else min(self.count(), stop)
        for i, key in enumerate(self.keys()):
            if i >= stop:
                rtn += " ..."
                break

            if i > 0:
                rtn += ", "

            rtn += str(key)

        rtn += "]\n"
        return rtn


    def print(self, *fields: str, stop: int=10, pretty: bool=True, file=sys.stdout) -> None:
        """Print the table content"""
        print(self.get_string(*fields, stop=stop, pretty=pretty), file=file)


    def __str__(self) -> str:
        return self.get_string(stop=10, pretty=True)


    def __repr__(self) -> str:
        return self.get_string(stop=10, pretty=False)

# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

class FWFIndexDict(FWFIndexLike[FWFSubset]):
    """An abstract base class defining the minimum methods and
    required core functionalities of a dict like index class
    """

    def __init__(self, parent: FWFViewLike, data: None|dict[Any, list[int]] = None):
        data = data if data is not None else FWFDict()
        super().__init__(parent, data)


    def to_T(self, value: list[int]) -> FWFSubset:      # pylint: disable=invalid-name
        return FWFSubset(self.parent, value)


    # Pylint has still issues with Generics. This is to prevent false positive warnings
    def __getitem__(self, key) -> FWFSubset:
        return super().__getitem__(key)


    def get_string(self, *fields: str, stop: int = 10, pretty: bool = True) -> str:
        """Get a string represention of the index"""

        rtn = f"{self.__class__.__name__}(count={self.count()}): ["

        stop = self.count() if stop < 0 else min(self.count(), stop)
        for i, (key, subset) in enumerate(self):
            if i >= stop:
                rtn += " ..."
                break

            if i > 0:
                rtn += ", "

            rtn += f"{str(key)}: len({len(subset)})"

        rtn += "]\n"
        return rtn


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

class FWFUniqueIndexDict(FWFIndexLike[FWFLine]):
    """An abstract base class defining the minimum methods and
    required core functionalities of a dict like index class
    """

    def __init__(self, parent: FWFViewLike, data: None|dict[Any, int] = None):
        data = data if data is not None else {}
        super().__init__(parent, data)


    def to_T(self, value: int) -> FWFLine:      # pylint: disable=invalid-name
        return self.parent.line_at(value)


    # Pylint has still issues with Generics. This is to prevent false positive warnings
    def __getitem__(self, key) -> FWFLine:
        return super().__getitem__(key)


    def get_pretty_string(self, *fields: str, stop: int = 10) -> str:
        """Create a string representation of the data"""
        stop = self.count() if stop < 0 else min(self.count(), stop)
        rtn = PrettyTable()

        rtn.field_names = fields or tuple(self.parent.field_getter.keys())
        gen = (tuple(row[v] for v in rtn.field_names) for i, (_, row) in enumerate(self) if i < stop)
        gen = list(gen)
        rtn.add_rows(gen)
        return rtn.get_string() + f"\n  len: {stop:,}/{self.count():,}"


    def get_string(self, *fields: str, stop: int = 10, pretty: bool = True) -> str:
        """Create a string representation of the data"""
        if pretty:
            return self.get_pretty_string(*fields, stop=stop)

        return super().get_string(*fields, stop=stop, pretty=pretty)
