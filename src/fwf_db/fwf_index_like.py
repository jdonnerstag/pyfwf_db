#!/usr/bin/env python
# encoding: utf-8

import abc
import collections.abc
from typing import Iterable, Iterator, Any, TypeVar, Generic

from .fwf_dict import FWFDict
from .fwf_view_like import FWFViewLike
from .fwf_subset import FWFSubset
from .fwf_line import FWFLine


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

class FWFIndexBuilder:
    """An Mixin defining some common core functionalities to build an index."""

    def index(self, fwfview: FWFViewLike, field: int|str, **kwargs):
        """A convience function to create the index without generator"""

        field = fwfview.field_from_index(field)

        # Create an iterator which iterates over all relevant records
        gen = self.index_generator(fwfview, field, **kwargs)

        # Do we need to apply any transformations...
        func = kwargs.get("func", None)
        if func is not None and callable(func):
            gen = (func(v) for v in gen)

        # Print some log-progress if requested, possible with every line
        log_progress = kwargs.get("log_progress", None)
        if log_progress is not None and callable(log_progress):
            gen = (log_progress(fwfview, i) or v for i, v in enumerate(gen))

        # Consume the iterator and create the index
        self.create_index_from_generator(fwfview, gen, **kwargs)

        return self


    def index_generator(self, fwfview: FWFViewLike, field: int|str, **_) -> Iterator[memoryview]:
        '''Provide an iterator (e.g generator) which iterates over all relevant records'''
        return fwfview.iter_lines_with_field(field)


    def create_index_from_generator(self, fwfview: FWFViewLike, gen: Iterator[memoryview], **kwargs) -> None:
        """Consume the iterator or generator and create the index"""


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

# 'T' will be either FWFLine or FWFSubset
T = TypeVar('T')

class FWFIndexLike(Generic[T], collections.abc.Mapping[Any, T]):
    """An abstract base class defining the minimum methods and
    required core functionalities of every index like class.
    """

    def __init__(self, fwfview: FWFViewLike, data: dict):
        self.fwfview = fwfview
        self.data: dict = data


    def keys(self) -> Iterable[Any]:
        """Return an iterable sequence of the index keys"""
        return self.data.keys()


    def __iter__(self) -> Iterator[tuple[Any, T]]:
        """Return an iterable sequence of the index keys"""
        yield from self.items()


    def __len__(self) -> int:
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


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

class FWFIndexDict(FWFIndexLike[FWFSubset]):
    """An abstract base class defining the minimum methods and
    required core functionalities of a dict like index class
    """

    def __init__(self, fwfview: FWFViewLike, data: None|dict[Any, list[int]] = None):
        data = data if data is not None else FWFDict()
        super().__init__(fwfview, data)


    def to_T(self, value: list[int]) -> FWFSubset:      # pylint: disable=invalid-name
        return FWFSubset(self.fwfview, value, self.fwfview.fields)


    # Pylint has still issues with Generics. This is to prevent false positive warnings
    def __getitem__(self, key) -> FWFSubset:
        return super().__getitem__(key)


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

class FWFUniqueIndexDict(FWFIndexLike[FWFLine]):
    """An abstract base class defining the minimum methods and
    required core functionalities of a dict like index class
    """

    def __init__(self, fwfview: FWFViewLike, data: None|dict[Any, int] = None):
        data = data if data is not None else {}
        super().__init__(fwfview, data)


    def to_T(self, value: int) -> FWFLine:      # pylint: disable=invalid-name
        return self.fwfview.line_at(value)


    # Pylint has still issues with Generics. This is to prevent false positive warnings
    def __getitem__(self, key) -> FWFLine:
        return super().__getitem__(key)
