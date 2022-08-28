#!/usr/bin/env python
# encoding: utf-8

from typing import Callable

from ._cython import fwf_db_cython
from .fwf_index_like import FWFDictIndexLike
from .fwf_file import FWFFile


class FWFCythonIndex(FWFDictIndexLike):
    """An index implementation, that leverages Cython for performance
    reasons. The larger the files, the larger are the performance gains.

    In case you know that your key is unique (as in Primary Key), then
    you can further improve the performance by using e.g. FWFCythonUniqueIndex.
    """

    def __init__(self, fwfview: FWFFile, field: int|str, func: None|Callable=None):
        assert isinstance(fwfview, FWFFile), f"FWFCythonIndex requires a FWFFile: {type(fwfview)}"

        super().__init__(fwfview, field)

        self.data = fwf_db_cython.create_index(self.fwfview, self.field)

        if func is not None:
            self.data = {func(k) : v for k, v in self.data.items()}
