#!/usr/bin/env python
# encoding: utf-8

import abc
import collections
from typing import Callable, Iterable, Iterator, Any, TypeVar, Generic

from .fwf_view_like import FWFViewLike
from .fwf_subset import FWFSubset
from .fwf_line import FWFLine


T = TypeVar('T')

class FWFIndexLike(Generic[T]):
    """An abstract base class defining the minimum methods and
    required core functionalities of every index class.
    """

    def __init__(self, fwfview: FWFViewLike, field: int|str):
        self.fwfview = fwfview
        self.field = fwfview.field_from_index(field)
        self.data: dict


    def index(self, func: None|Callable = None, log_progress: None|Callable = None):
        """A convience function to create the index without generator"""

        # Create an iterator which iterates over all relevant records
        gen = self._index1()

        # Do we need to apply any transformations...
        if func:
            gen = ((i, func(v)) for i, v in gen)

        # Print some log-progress if requested, possible with every line
        if log_progress is not None:
            view = self.fwfview
            gen = (log_progress(view, i) or (i, v) for i, v in gen)

        # Consume the iterator and create the index
        self._index2(gen)

        return self


    def _index1(self) -> Iterator[tuple[int, bytes]]:
        '''Provide an iterator (e.g generator) which iterates over all relevant records'''
        return self.fwfview.iter_lines_with_field(self.field)


    def _index2(self, gen):
        """Consume the iterator or generator and create the index"""
        return gen


    def keys(self) -> Iterable[Any]:
        """Return an iterable sequence of the index keys"""
        return self.data.keys()


    def __iter__(self) -> Iterator[tuple[Any, T]]:
        """Return an iterable sequence of the index keys"""
        yield from self.items()


    def __len__(self) -> int:
        return len(self.data.keys())


    def __getitem__(self, key) -> T:
        """Create a new view with all rows matching the index key"""
        value = self.get(key)
        if value is not None:
            return value

        raise KeyError(f"Key not found: {key}")


    @abc.abstractmethod
    def get(self, key, default=None) -> None | T:
        """Create a new view with all rows matching the index key"""


    def items(self) -> Iterator[tuple[Any, T]]:
        """Iterate over all key/value tuples"""

        for key in self.data:
            yield key, self[key]


    def __contains__(self, param: Any) -> bool:
        return param in self.data


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

class FWFDictIndexLike(FWFIndexLike[FWFSubset]):
    """An abstract base class defining the minimum methods and
    required core functionalities of a dict like index class
    """

    def __init__(self, fwfview: FWFViewLike, field: int|str):
        super().__init__(fwfview, field)
        self.data: dict[Any, list[int]] = collections.defaultdict(list)


    def get(self, key, default=None) -> None | FWFSubset:
        """Create a view based on the indices associated with the index key provided"""

        # self.data is a defaultdict, hence the additional 'in' test
        if key not in self.data:
            return default

        return FWFSubset(self.fwfview, self.data[key], self.fwfview.fields)


    # Pylint has still issues with Generics. This is to prevent false positive warnings
    def __getitem__(self, key) -> FWFSubset:
        return super().__getitem__(key)


# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

class FWFDictUniqueIndexLike(FWFIndexLike[FWFLine]):
    """An abstract base class defining the minimum methods and
    required core functionalities of a dict like index class
    """

    def __init__(self, fwfview: FWFViewLike, field: int|str):
        super().__init__(fwfview, field)
        self.data: dict[Any, int] = {}


    def get(self, key, default=None) -> None|FWFLine:
        """Create a view based on the indices associated with the index key provided"""
        if key not in self.data:
            return default

        idx = self.data[key]
        return self.fwfview.line_at(idx)


    # Pylint has still issues with Generics. This is to prevent false positive warnings
    def __getitem__(self, key) -> FWFLine:
        return super().__getitem__(key)
