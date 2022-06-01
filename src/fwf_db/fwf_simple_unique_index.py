#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFDictIndexLike
from .fwf_line import FWFLine


class FWFSimpleUniqueIndexException(Exception):
    pass


class FWFSimpleUniqueIndex(FWFDictIndexLike):
    """A simple unique index implementation, based on pure python"""

    def __init__(self, fwfview):

        self.init_dict_index_like(fwfview)

        self.field = None   # The field name to build the index
        self.data = None    # dict(value -> (last) lineno)


    def _index2(self, gen):
        # Create the index
        self.data = {value : i for i, value in gen}


    def fwf_subset(self, fwfview, key, fields):
        if key in self.data:
            lineno = self.data[key]
            return FWFLine(fwfview, lineno, fwfview.line_at(lineno))
