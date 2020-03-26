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
        self.data = None    # The Pandas groupby result


    def _index(self, field, dtype=None, func=None, chunksize=None):
        """Create the Index
        
        The 'field' to base the index upon
        'np_type' is the Numpy dtype used to create the array that initially
        holds the index data.
        'func' to process the field data before adding it to the index, e.g. 
        lower, upper, str, int, etc.. 
        
        """

        assert dtype, "Missing parameter: dtype"

        # Create the full size index all at once => number of records        
        reclen = len(self.fwfview)
        values = np.empty(reclen, dtype=dtype)

        # Determine the slice information to retrieve the field from the line
        fields = self.fwfview.fields
        if isinstance(field, int):
            field = next(islice(fields.keys(), field, None))

        sslice = fields[field]

        # Iterate over all rows in the view
        for i, line in self.fwfview.iter_lines():
            value = line[sslice]
            if func:
                value = func(value)

            values[i] = value

            if chunksize:
                yield i

        # All the user to process the index data before they are grouped.
        df = pd.DataFrame(values, columns=["values"])
        df = self.cleanup_df(df)
        groups = df.groupby("values")
        self.data = groups


    def cleanup_df(self, df):
        """Replace the implementation with something useful to process the 
        index data before they are grouped.
        """
        return df


    def str(self, df, encoding="utf-8"):
        """Convert the index data into strings, optionally decoding them using 
        the encoding provided
        """
        df["values"] = df["values"].str.decode(encoding=encoding)
        return df


    def strip(self, df, encoding=None):
        """Convert the index data into string dtypes and strip spaces from 
        both ends.
        """
        df["values"] = df["values"].astype("string").str.strip()
        return df


    def int(self, df, dtype):
        """Convert the index data from binary into int (actually any valid
        dtype provided)
        """
        df["values"] = df["values"].astype(dtype)
        return df


    def __len__(self):
        """The number entries (unique values) in the index"""
        return self.data.ngroups


    def __iter__(self):
        """Iterate over all index keys"""
        iter(self.data.keys())


    def fwf_subset(self, fwffile, key, fields):
        """Create a view with the indices associated with the index key provided"""
        rtn = self.data.groups.get(key)
        if rtn is not None:
            return FWFSubset(fwffile, rtn, fields)


    def __contains__(self, param):
        return param in self.data.groups
