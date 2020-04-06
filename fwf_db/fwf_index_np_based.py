#!/usr/bin/env python
# encoding: utf-8

import numpy as np
import pandas as pd 
from itertools import islice

from .fwf_index_like import FWFIndexLike
from .fwf_subset import FWFSubset


class FWFIndexNumpyBased(FWFIndexLike):
    """A Numpy and Pandas based Index
    
    Especially with large files with millions of records in the index, 
    a Pandas based index is (much) faster compared to pure python based on. 
    """ 

    def __init__(self, fwfview):
        self.fwfview = fwfview
        self.field = None   # The field to use for the index
        self.dtype = None
        self.data = None    # The Pandas groupby result
        self.keys = None


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

        df = self._index2a(gen)

        if self.cleanup_df is not None:
            df["values"] = self.cleanup_df(df["values"])

        self.data = groups = self._index2b(df)
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

        u, indices = np.unique(values, return_index=True)        

        return pd.DataFrame(values, columns=["values"])


    def _index2b(self, df):
        self.keys = pd.unique(df["values"])

        df["index"] = df.index
        df = df.set_index("values")

        return df


    def __len__(self):
        """The number entries (unique values) in the index"""
        return len(self.keys)


    def __iter__(self):
        """Iterate over all index keys"""
        iter(self.keys)


    def fwf_subset(self, fwffile, key, fields):
        """Create a view with the indices associated with the index key provided"""
        rtn = self.data.get_group(key)
        if rtn is not None:
            return FWFSubset(fwffile, rtn, fields)


    def __contains__(self, param):
        return param in self.keys
