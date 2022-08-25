#!/usr/bin/env python
# encoding: utf-8

from typing import Iterator
from collections import defaultdict

from .fwf_index_like import FWFDictIndexLike
from .fwf_subset import FWFSubset


class FWFSimpleIndexException(Exception):
    """ FWFSimpleIndexException """


class FWFSimpleIndex(FWFDictIndexLike):
    """A simple index implementation, based on pure python"""

    def __init__(self, fwfview):

        self.init_dict_index_like(fwfview)

        self.field = None   # The field name to build the index
        self.data = {}      # dict(value -> [lineno])


    def _index2(self, gen: Iterator[tuple[int, bytes]]):
        # Create the index
        self.data = values = defaultdict(list)
        all(values[value].append(i) or True for i, value in gen)
        # TODO May be should do self.data=dict(self.data) when done with creating the index? How do we know?


    def fwf_subset(self, fwfview, key, fields) -> FWFSubset:
        """Create a view based on the indices associated with the index key provided"""

        # self.data is a defaultdict, hence the additional 'in' test
        if key in self.data:
            value = self.data[key]
            return FWFSubset(fwfview, value, fields)

        raise IndexError(f"'key' not found in Index: {key}")


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
