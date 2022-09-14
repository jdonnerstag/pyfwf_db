#!/usr/bin/env python
# encoding: utf-8

"""A very simple index implementation"""

from typing import Iterator

from .fwf_index_like import FWFIndexLike, FWFIndexBuilder
from .fwf_view_like import FWFViewLike


class FWFSimpleIndexBuilder(FWFIndexBuilder):
    """A simple index implementation, based on pure python"""

    def __init__(self, data: FWFIndexLike):
        self.data = data


    def create_index_from_generator(self, fwfview: FWFViewLike, gen: Iterator[bytes], **kwargs) -> None:
        # TODO May be move to cython for speed?
        for i, value in enumerate(gen):
            self.data[value] = i
