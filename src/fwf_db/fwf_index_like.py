#!/usr/bin/env python
# encoding: utf-8

import abc
from typing import Callable, Iterable, Iterator, Union, Any, TYPE_CHECKING

from .fwf_base_mixin import FWFBaseMixin

if TYPE_CHECKING:
    from .fwf_subset import FWFSubset
    from .fwf_line import FWFLine


class FWFIndexLike(FWFBaseMixin, abc.ABC):
    """An abstract base class defining the minimum methods and
    required core functionalities of every index class
    """

    # TODO Why not init()
    # TODO May be add 'name' attribute to improve error messages?
    def init_index_like(self, fwfview):
        """Initialize the mixin"""
        self.fwfview = fwfview
        self.field = None


    def index(self, field, func=None, log_progress: None|Callable = None):
        """A convience function to create the index without generator"""

        gen = self._index1(self.fwfview, field, func)

        if log_progress is not None:
            view = self.fwfview
            view.progress_count = 0
            gen = (log_progress(view, i) or (i, v) for i, v in gen)

        self._index2(gen)

        return self


    def _index2(self, gen):
        """Create the index"""


    @abc.abstractmethod
    def __len__(self) -> int:
        """Provide the number of entries in the index"""


    @abc.abstractmethod
    def __iter__(self) -> Iterator['FWFLine']:
        """Iterate over all rows in the index"""


    def __getitem__(self, key) -> Union['FWFSubset', 'FWFLine']:
        """Create a new view with all rows matching the index key"""
        return self.get(key)


    @abc.abstractmethod
    def get(self, key) -> Union['FWFSubset', 'FWFLine']:
        """Create a new view with all rows matching the index key"""


    @abc.abstractmethod
    def __contains__(self, param) -> bool:
        """True if param is a key in the index"""

# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

class FWFDictIndexLike(FWFIndexLike):
    """An abstract base class defining the minimum methods and
    required core functionalities of a dict like index class
    """

    def init_dict_index_like(self, fwfview):
        """Initialize the mixin"""

        self.init_index_like(fwfview)
        self.data: dict[Any, list[int]] = {}


    def __len__(self) -> int:
        """The number of index keys"""
        return len(self.data.keys())


    def keys(self) -> Iterable:
        return self.data.keys()


    def __iter__(self) -> Iterator[tuple[Any, 'FWFSubset']]:
        """Iterate over the index keys"""
        yield from self.items()


    def __getitem__(self, key) -> 'FWFSubset':
        """Create a new view with all rows matching the index key"""
        return self.get(key)


    @abc.abstractmethod
    def get(self, key) -> 'FWFSubset':
        """Create a new view with all rows matching the index key"""


    def items(self) -> Iterator[tuple[Any, 'FWFSubset']]:
        for key in self.data:
            yield key, self.get(key)


    def __contains__(self, param) -> bool:
        return param in self.data

# ---------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------

class FWFDictUniqueIndexLike(FWFIndexLike):
    """An abstract base class defining the minimum methods and
    required core functionalities of a dict like index class
    """

    def init_dict_index_like(self, fwfview):
        """Initialize the mixin"""

        self.init_index_like(fwfview)
        self.data: dict[Any, int] = {}


    def __len__(self) -> int:
        """The number of index keys"""
        return len(self.data.keys())


    def keys(self) -> Iterable:
        return self.data.keys()


    def __iter__(self) -> Iterator[tuple[Any, 'FWFLine']]:
        """Iterate over the index keys"""
        yield from self.items()


    def __getitem__(self, key) -> 'FWFLine':
        """Create a new view with all rows matching the index key"""
        return self.get(key)


    @abc.abstractmethod
    def get(self, key) -> 'FWFLine':
        """Create a new view with all rows matching the index key"""


    def items(self) -> Iterator[tuple[Any, 'FWFLine']]:
        for key in self.data:
            yield key, self.get(key)


    def __contains__(self, param) -> bool:
        return param in self.data
