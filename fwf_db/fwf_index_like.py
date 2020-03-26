#!/usr/bin/env python
# encoding: utf-8

import abc


class FWFIndexLike(abc.ABC):
    """An abstract base class defining the minimum methods and
    required core functionalities of every index class
    """

    def init_index_like(self, fwfview):
        """Initialize the mixin"""
        self.fwfview = fwfview
        self.field = None


    def index(self, field, func=None, **kvargs):
        """A convience function to create the index without generator"""
        for _ in self._index(field, func=func, **kvargs):
            """  """

        return self


    @abc.abstractmethod
    def _index(self, field, func=None, chunksize=None):
        """Create an index for data in column 'field'. 

        Optionally the value can be transformed, e.g. string, int, date, 
        bevor adding it to the index.
        """
        pass


    @abc.abstractmethod
    def __len__(self):
        """Provide the number of entries in the index"""
        pass


    @abc.abstractmethod
    def __iter__(self):
        """Iterate over all rows in the index"""
        pass


    @abc.abstractmethod
    def fwf_subset(self, fwffile, key, fields):
        """Create a new view based on range (slice) provided"""
        pass


    def __getitem__(self, key):
        """Create a new view with all rows matching the index key"""
        return self.fwf_subset(self.fwfview, key, self.fwfview.fields)


    def get(self, key):
        """Create a new view with all rows matching the index key"""
        return self.fwf_subset(self.fwfview, key, self.fwfview.fields)


    @abc.abstractmethod
    def __contains__(self, param):
        """True if param is a key in the index"""
