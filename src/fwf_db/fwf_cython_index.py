#!/usr/bin/env python
# encoding: utf-8

import collections
from typing import Callable

from ._cython import fwf_db_cython
from .fwf_index_like import FWFDictIndexLike
from .fwf_file import FWFFile
from .fwf_multi_file import FWFMultiFile


class FWFCythonIndex(FWFDictIndexLike):
    """An index implementation, that leverages Cython for performance
    reasons. The larger the files, the larger are the performance gains.

    In case you know that your key is unique (as in Primary Key), then
    you can further improve the performance by using e.g. FWFCythonUniqueIndex.
    """

    def __init__(self, fwfview: FWFFile|FWFMultiFile, field: int|str, func: None|Callable=None):
        super().__init__(fwfview, field, collections.defaultdict(list))

        if isinstance(fwfview, FWFFile):
            fwf_db_cython.create_index(self.fwfview, self.field, self.data)
        elif isinstance(fwfview, FWFMultiFile):
            offset = 0
            for file in fwfview.files:
                fwf_db_cython.create_index(file, self.field, self.data, offset)
                offset += file.line_count
        else:
            raise TypeError(f"FWFCythonIndex requires either a FWFFile or FWFMultiFile: {type(fwfview)}")

        if func is not None:
            self.data = {func(k) : v for k, v in self.data.items()}
