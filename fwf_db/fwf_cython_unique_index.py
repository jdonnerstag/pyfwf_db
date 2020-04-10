#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice
import numpy as np

from .fwf_index_like import FWFIndexLike
from .fwf_line import FWFLine
from .cython import fwf_db_ext
from .fwf_simple_index import FWFSimpleIndex


class FWFCythonUniqueIndex(FWFIndexLike):
    """Performance can be further improved if we can assume that the PK
    is unique. Considering effective dates etc. there might still be
    multiple records with the same PK in the data set, but only the 
    last one is valid. 
    """

    def __init__(self, fwfview):

        self.fwfview = fwfview
        self.field = None   # The field name to build the index
        self.data = {}    # dict(value -> lineno)


    def index(self, field, func=None, log_progress=None):
        """A convience function to create the index without generator"""

        assert log_progress is None, f"Parameter 'log_progress' is not supported with this Indexer"

        field = self.fwfview.field_from_index(field)
        self.data = fwf_db_ext.create_unique_index(self.fwfview, field)

        if func is not None:
            self.data = {func(k) : v for k, v in self.data.items()}
    
        return self


    def _index2(self, gen):
        raise Exception("Method not implemented / required in this class. Do not invoke it")        


    def __len__(self):
        """The number of index keys"""
        return len(self.data.keys())


    def __iter__(self):
        """Iterate over the index keys"""
        return iter(self.data.keys())


    def fwf_subset(self, fwfview, key, fields):
        """Create a view based on the indices associated with the index key provided""" 
        if key in self.data:
            idx = self.data[key]
            return FWFLine(fwfview, idx, fwfview.line_at(idx))

    def __contains__(self, param):
        return param in self.data
