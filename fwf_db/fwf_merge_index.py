#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFIndexLike
from .fwf_multi_subset import FWFMultiSubset


class FWFMergeIndexException(Exception):
    pass


class FWFMergeIndex(FWFIndexLike):

    def __init__(self):

        self.fwfview = None
        self.field = None   # The field name to build the index
        self.indices = []
        self.data = defaultdict(list)    # dict(value -> [lineno])


    def merge(self, index):

        idx = len(self.indices)

        if getattr(index, "data", None) is None:
            raise FWFMergeIndexException(
                f"'index' must be of type by an Index with 'data' attribute")

        self.indices.append(index)
        
        if not self.fwfview:
            self.fwfview = index.fwfview

        for k, v in index.data.items():
            all(self.data[k].append((idx, x)) or True for x in v)

        return self


    def __len__(self):
        """The number of index keys"""
        return len(self.data.keys())


    def __iter__(self):
        """Iterate over the index keys"""
        return iter(self.data.keys())


    def fwf_subset(self, fwfview, key, fields):
        """Create a view based on the indices associated with the index key provided""" 
        
        # self.data is a defaultdict(list) and thus requesting an entry that
        # does not exist would create a new entry. We don't want that ...
        if key in self.data:
            return FWFMultiSubset(self.indices, self.data[key])


    def __contains__(self, param):
        return param in self.data
