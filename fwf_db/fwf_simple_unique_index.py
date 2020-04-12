#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFIndexLike
from .fwf_line import FWFLine


class FWFSimpleUniqueIndex(FWFIndexLike):
    """A simple unique index implementation, based on pure python"""

    def __init__(self, fwfview):

        self.fwfview = fwfview
        self.field = None   # The field name to build the index
        self.data = None    # dict(value -> (last) lineno)


    def _index2(self, gen):
        # Create the index
        self.data = {value : i for i, value in gen}


    def __len__(self):
        """The number of index keys"""
        return len(self.data.keys())


    def __iter__(self):
        """Iterate over the index keys"""
        return iter(self.data.keys())


    def fwf_subset(self, fwfview, key, fields):
        if key in self.data:
            lineno = self.data[key]
            return FWFLine(fwfview, lineno, fwfview.line_at(lineno))


    def __contains__(self, param):
        return param in self.data
