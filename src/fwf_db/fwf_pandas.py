#!/usr/bin/env python
# encoding: utf-8

import pandas as pd

from .fwf_view_like import FWFViewLike
from .fwf_file import FWFFile


class FWFPandas:
    """Export the data to Pandas"""

    def to_pandas(self, fwfview: FWFViewLike, dtype=None) -> pd.DataFrame:
        """Load fields denoted by dtype into a Pandas dataframe. Please not, Pandas
        does still need a lot of memory
        """

        if dtype is None:
            parent = fwfview
            while True:
                newp = parent.get_parent()
                if newp is None:
                    break
                parent = newp

            assert isinstance(parent, FWFFile)
            fieldspec = parent.fieldspecs
            dtype = {e["name"] : e.get("dtype", None) for e in fieldspec}
        elif isinstance(dtype, list):
            list_of_strings = all(isinstance(x, str) for x in dtype)
            if list_of_strings:
                dtype = {x : None for x in dtype}
            else:
                # In case a fieldspec has been provided...
                dtype = {e["name"] : e["dtype"] for e in dtype if "dtype" in e}

        names = list(dtype.keys())
        strs = [ftype in ["str", "string"] for ftype in dtype.items()]

        gen = (line.to_list(names) for line in fwfview)
        # TODO fwf_file may deserve a function to create a dataclass record from a FWFLine
        gen = ([line[i].decode("utf-8") if strs[i] else line[i] for i in range(len(strs))] for line in gen)

        df = pd.DataFrame.from_records(gen, columns=names, index=None)

        for field, ftype in dtype.items():
            if ftype:
                df[field] = df[field].astype(ftype)

        return df
