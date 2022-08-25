#!/usr/bin/env python
# encoding: utf-8

from typing import Any, Sequence
from .fwf_index_like import FWFDictUniqueIndexLike
from .fwf_line import FWFLine


class FWFSimpleUniqueIndexException(Exception):
    ''' FWFSimpleUniqueIndexException '''


class FWFSimpleUniqueIndex(FWFDictUniqueIndexLike):
    """A simple unique index implementation, based on pure python"""

    def __init__(self, fwfview):

        self.init_dict_index_like(fwfview)

        self.field = None   # The field name to build the index
        self.data: dict[Any, int] = {}     # dict(value -> (last) lineno)


    def _index2(self, gen: Sequence[tuple[int, Any]]):
        # Create the index
        self.data = {value : i for i, value in gen}


    def get(self, key) -> FWFLine:
        if key in self.data:
            lineno = self.data[key]
            return FWFLine(self.fwfview, lineno, self.fwfview.line_at(lineno))

        raise IndexError(f"'key' not found in Index: {key}")
