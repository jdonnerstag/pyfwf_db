#!/usr/bin/env python
# encoding: utf-8

"""A very simple index implementation"""

from typing import Iterator, Callable
from collections import defaultdict

from .fwf_index_like import FWFDictIndexLike
from .fwf_view_like import FWFViewLike


class FWFSimpleIndex(FWFDictIndexLike):
    """A simple index implementation, based on pure python"""

    def __init__(self,
        fwfview: FWFViewLike,
        field: int|str,
        func: None|Callable=None,
        log_progress: None|Callable = None):

        super().__init__(fwfview, field)
        self.index(func, log_progress)


    def _index2(self, gen: Iterator[bytes]):
        # Create the index
        self.data = values = defaultdict(list)
        all(values[value].append(i) or True for i, value in enumerate(gen))
        # TODO May be should do self.data=dict(self.data) when done with creating the index? How do we know?
