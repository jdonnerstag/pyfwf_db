#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFIndexLike
from .fwf_subset import FWFSubset


class FWFSimpleIndex(FWFIndexLike):
    """A simple index implementation, based on pure python"""

    def __init__(self, fwfview):

        self.fwfview = fwfview
        self.field = None   # The field name to build the index
        self.data = {}    # dict(value -> [lineno])


    def _index2(self, gen):
        # Create the index
        self.data = values = defaultdict(list)
        all(values[value].append(i) or True for i, value in gen)


    def __len__(self):
        """The number of index keys"""
        return len(self.data.keys())


    def __iter__(self):
        """Iterate over the index keys"""
        return iter(self.data.keys())


    def fwf_subset(self, fwfview, key, fields):
        """Create a view based on the indices associated with the index key provided""" 
        if key in self.data:
            return FWFSubset(fwfview, self.data[key], fields)


    def __contains__(self, param):
        return param in self.data
        