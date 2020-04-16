#!/usr/bin/env python
# encoding: utf-8

import numpy as np
import pandas as pd 
from itertools import islice
from collections import defaultdict

from .fwf_index_like import FWFDictIndexLike
from .fwf_subset import FWFSubset


class FWFIndexNumpyBased(FWFDictIndexLike):
    """A Numpy and Pandas based Index
    
    Especially with large files with millions of records in the index, 
    a Pandas based index is (much) faster compared to pure python based on. 
    """ 

    def __init__(self, fwfview):

        self.init_dict_index_like(fwfview)
        self.field = None   # The field to use for the index
        self.dtype = None
        self.data = None    # defaultdict key -> [<indices>]


    def index(self, field, dtype=None, func=None, log_progress=None, cleanup_df=None):
        if dtype is None:
            dtype = self.fwfview.field_dtype(1)

        self.dtype = dtype
        self.cleanup_df = cleanup_df

        super().index(field, func, log_progress)
        return self


    def _index2(self, gen):
        """Create the Index
        
        The 'field' to base the index upon
        'np_type' is the Numpy dtype used to create the array that initially
        holds the index data.
        'func' to process the field data before adding it to the index, e.g. 
        lower, upper, str, int, etc.. 
        """

        values = self._index2a(gen)

        if self.cleanup_df is not None:
            values = self.cleanup_df(values)

        self.data = groups = self._index2b(values)
        return groups


    def _index2a(self, gen):
        """Create the Index
        df.
        The 'field' to base the index upon
        'np_type' is the Numpy dtype used to create the array that initially
        holds the index data.
        'func' to process the field data before adding it to the index, e.g. 
        lower, upper, str, int, etc.. 
        """

        # Create the full size index all at once => number of records        
        reclen = len(self.fwfview)
        values = np.empty(reclen, dtype=self.dtype)

        for i, value in gen:
            values[i] = value

        return values


    def _index2b(self, values):
        # I tested all sort of numpy and pandas ways, but nothing was as
        # fast as python generators. Any test needs to consider (a) how
        # long it takes to create the "index" and (b) how long it takes
        # to lookup values. For any meaningful performance indication, (a) 
        # the array must have at least 10 mio entries and (b) you must 
        # execute at least 1 mio lookups against the 10 mio entties.
        data = defaultdict(list)
        all(data[value].append(i) or True for i, value in enumerate(values))
        return data
        

    def fwf_subset(self, fwffile, key, fields):
        """Create a view with the indices associated with the index key provided"""
        if key in self.data:
            return FWFSubset(fwffile, self.data[key], fields)
