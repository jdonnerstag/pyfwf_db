#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFIndexLike
from .fwf_subset import FWFSubset


class FWFMergeIndexException(Exception):
    pass


class FWFMergeIndex(FWFIndexLike):

    def __init__(self):

        self.field = None   # The field name to build the index
        self.indices = []
        self.data = defaultdict(list)    # dict(value -> [lineno])


    def merge(self, index):

        idx = len(self.indices)

        if getattr(index, "data", None) is None:
            raise FWFMergeIndexException(
                f"'index' must be of type by an Index with 'data' attribute")

        self.indices.append(index)

        for k, v in index.data.items():
            all(self.data[k].append((idx, x)) or True for x in v)

        return self


    def _index2(self, gen):
        pass


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
