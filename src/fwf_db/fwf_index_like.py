#!/usr/bin/env python
# encoding: utf-8

import abc

from .fwf_base_mixin import FWFBaseMixin


class FWFIndexLike(FWFBaseMixin, abc.ABC):
    """An abstract base class defining the minimum methods and
    required core functionalities of every index class
    """

    def init_index_like(self, fwfview):
        """Initialize the mixin"""
        self.fwfview = fwfview
        self.field = None


    def index(self, field, func=None, log_progress=None):
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
    def __len__(self):
        """Provide the number of entries in the index"""


    @abc.abstractmethod
    def __iter__(self):
        """Iterate over all rows in the index"""


    @abc.abstractmethod
    def fwf_subset(self, fwffile, key, fields):
        """Create a new view based on range (slice) provided"""


    def __getitem__(self, key):
        """Create a new view with all rows matching the index key"""
        return self.get(key)


    def get(self, key):
        """Create a new view with all rows matching the index key"""
        return self.fwf_subset(self.fwfview, key, self.fwfview.fields)


    @abc.abstractmethod
    def __contains__(self, param):
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
        self.data = {}


    def __len__(self):
        """The number of index keys"""
        return len(self.data.keys())


    def keys(self):
        return self.data.keys()


    def __iter__(self):
        """Iterate over the index keys"""
        yield from self.items()


    def items(self):
        for key in self.data.keys():
            data = self.fwf_subset(self.fwfview, key, self.fwfview.fields)
            yield key, data


    def __contains__(self, param):
        return param in self.data
