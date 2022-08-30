#!/usr/bin/env python
# encoding: utf-8

from typing import Callable
from .fwf_file import FWFFile
from .fwf_index_like import FWFDictUniqueIndexLike
from ._cython import fwf_db_cython


class FWFCythonUniqueIndex(FWFDictUniqueIndexLike):
    """Performance can be further improved if we can assume that the PK
    is unique. Considering effective dates etc. there might still be
    multiple records with the same PK in the data set, but only the
    last one is valid.

    The implementation does not check if the index is really unique. It
    simple takes the last entry it can find.
    """

    def __init__(self, fwfview: FWFFile, field: int|str, func: None|Callable=None):
        assert isinstance(fwfview, FWFFile), f"FWFCythonUniqueIndex requires a FWFFile: {type(fwfview)}"

        super().__init__(fwfview, field)

        self.data = fwf_db_cython.create_unique_index(self.fwfview, self.field)

        if func is not None:
            self.data = {func(k) : v for k, v in self.data.items()}


    # Implement abstract method, but it is not used
    def _index2(self, gen):
        pass