#!/usr/bin/env python
# encoding: utf-8

from typing import Any, Callable
from .fwf_index_like import FWFDictUniqueIndexLike
from .fwf_line import FWFLine
from ._cython import fwf_db_cython


class FWFCythonUniqueIndexException(Exception):
    ''' FWFCythonUniqueIndexException '''


class FWFCythonUniqueIndex(FWFDictUniqueIndexLike):
    """Performance can be further improved if we can assume that the PK
    is unique. Considering effective dates etc. there might still be
    multiple records with the same PK in the data set, but only the
    last one is valid.

    The implementation does not check if the index is really unique. It
    simple takes the last entry it can find.
    """

    def __init__(self, fwfview):

        self.init_dict_index_like(fwfview)
        self.field = None   # The field name to build the index
        self.data: dict[Any, int] = {}      # dict(value -> lineno)

        if getattr(self.fwfview, "mm", None) is None:
            raise FWFCythonUniqueIndexException(f"Only FWFile parent are supported with {type(self)}")


    def index(self, field, func:None|Callable=None, log_progress: None|Callable = None):
        """A convience function to create the index without generator"""

        assert log_progress is None, "Parameter 'log_progress' is not supported with this Indexer"

        field = self.fwfview.field_from_index(field)
        self.data = fwf_db_cython.create_unique_index(self.fwfview, field)

        if func is not None:
            self.data = {func(k) : v for k, v in self.data.items()}

        return self


    def get(self, key) -> FWFLine:
        """Create a view based on the indices associated with the index key provided"""
        if key in self.data:
            idx = self.data[key]
            return FWFLine(self.fwfview, idx, self.fwfview.line_at(idx))

        raise IndexError(f"'key' not found in Index: {key}")
