#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFDictIndexLike
from .fwf_subset import FWFSubset


class FWFSimpleIndexException(Exception):
    pass


class FWFSimpleIndex(FWFDictIndexLike):
    """A simple index implementation, based on pure python"""

    def __init__(self, fwfview):

        self.init_dict_index_like(fwfview)

        self.field = None   # The field name to build the index
        self.data = {}    # dict(value -> [lineno])


    def _index2(self, gen):
        # Create the index
        self.data = values = defaultdict(list)
        all(values[value].append(i) or True for i, value in gen)


    def fwf_subset(self, fwfview, key, fields):
        """Create a view based on the indices associated with the index key provided""" 
        if key in self.data:
            return FWFSubset(fwfview, self.data[key], fields)
        

    def delevel(self):
        """In case the index has been created on top of a view, then it is 
        possible to reduce the level of indirection by one.
        """
        # The current implementation is rather specific and may not work with
        # all kind of parents.

        data = {key : [self.fwfview.lines[i] for i in values] for key, values in self.data.items()}

        rtn = FWFSimpleIndex(self.fwfview.fwffile)
        rtn.data = data
        rtn.field = self.field

        return rtn
