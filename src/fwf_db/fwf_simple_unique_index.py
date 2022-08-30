#!/usr/bin/env python
# encoding: utf-8

from typing import Iterator, Callable

from .fwf_index_like import FWFDictUniqueIndexLike
from .fwf_view_like import FWFViewLike


class FWFSimpleUniqueIndex(FWFDictUniqueIndexLike):
    """A simple unique index implementation, based on pure python"""

    def __init__(self, fwfview: FWFViewLike, field: int|str, func: None|Callable=None, log_progress: None|Callable = None):
        super().__init__(fwfview, field)
        self.index(func, log_progress)


    def _index2(self, gen: Iterator[bytes]):
        # Create the index
        self.data = {value : i for i, value in enumerate(gen)}
