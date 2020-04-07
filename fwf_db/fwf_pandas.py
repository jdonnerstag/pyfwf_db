#!/usr/bin/env python
# encoding: utf-8


import numpy as np
import pandas as pd

from .fwf_base_mixin import FWFBaseMixin


class FWFPandas(FWFBaseMixin):
    """Export the data to Pandas"""
    
    def __init__(self, fwffile):
        self.fwffile = fwffile


    def to_pandas(self, dtype=None):
        """Load fields denoted by dtype into a Pandas dataframe. Please not, Pandas
        does still need a lot of memory
        """

        if dtype is None:
            parent = self.fwffile
            while getattr(parent, "fieldspecs", None) is None:
                parent = parent.fwffile

            fieldspec = parent.fieldspecs
            dtype = {e["name"] : e.get("dtype", None) for e in fieldspec}
        elif isinstance(dtype, list):
            list_of_strings = all(isinstance(x, str) for x in dtype)
            if list_of_strings:
                dtype = {x : None for x in dtype}
            else:
                # In case a fieldspec has been provided...
                dtype = {e["name"] : e["dtype"] for e in dtype if "dtype" in e}

        names = dtype.keys()
        strs = [ftype in ["str", "string"] for ftype in dtype.items()]

        df = pd.DataFrame(index=None)
        gen = (line.to_list(names) for line in self.fwffile)
        gen = ([line[i].decode("utf-8") if strs[i] else line[i] for i in range(len(strs))] for line in gen)

        df = pd.DataFrame(gen, columns=names, index=None)

        for field, ftype in dtype.items():
            if ftype:
                df[field] = df[field].astype(ftype)

        return df
