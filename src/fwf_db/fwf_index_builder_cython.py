#!/usr/bin/env python
# encoding: utf-8

"""Define an index which leverages a small piece of cython code
to speed up index creation."""

from typing import Callable

from ._cython import fwf_db_cython
from .fwf_index_like import FWFIndexLike
from .fwf_file import FWFFile
from .fwf_multi_file import FWFMultiFile


class FWFCythonIndexBuilder:
    """An index implementation, that leverages Cython for performance
    reasons. The larger the files, the larger are the performance gains.

    Depending on the 'unique' argument, either a unique or none-unique
    index will be created. None-unique indexes are using lists to hold
    multiple values.
    """

    def __init__(self, data: FWFIndexLike):
        self.data = data


    def index(self, fwfview: FWFFile|FWFMultiFile, field: int|str, func: None|Callable=None):
        """Create the index"""

        field = fwfview.field_from_index(field)

        if isinstance(fwfview, FWFFile):
            fwf_db_cython.create_index(fwfview, field, self.data, func=func)
        elif isinstance(fwfview, FWFMultiFile):
            offset = 0
            for file in fwfview.files:
                fwf_db_cython.create_index(file, field, self.data, offset, func=func)
                offset += file.line_count
        else:
            raise TypeError(f"FWFCythonIndex requires either a FWFFile or FWFMultiFile: {type(fwfview)}")
