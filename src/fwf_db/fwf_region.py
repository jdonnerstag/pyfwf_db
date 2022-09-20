#!/usr/bin/env python
# encoding: utf-8

"""Define a view that represents a region of its parent view"""

from typing import Iterator

from .fwf_view_like import FWFViewLike
from .fwf_subset import FWFSubset


class FWFRegion(FWFViewLike):
    """A view on the parents data based on a slice with start
    and stop indexes
    """

    def __init__(self, fwffile: FWFViewLike, start: int, stop: int, fields):
        super().__init__(fields)

        assert start >= 0
        assert start <= stop

        self.parent = fwffile
        self.start = start
        self.stop = stop


    def __len__(self) -> int:
        return self.stop - self.start


    def get_parent(self) -> 'FWFViewLike':
        return self.parent


    def _parent_index(self, index: int) -> int:
        return self.start + index


    def _raw_line_at(self, index: int) -> memoryview:
        index = self.parent_index(index)
        return self.get_parent().raw_line_at(index)


    def _fwf_by_indices(self, indices: list[int]) -> FWFSubset:
        indices = [self.parent_index(i) for i in indices]
        return FWFSubset(self.parent, indices, self.fields)


    def _fwf_by_slice(self, start: int, stop: int) -> 'FWFRegion':
        start = self._normalize_index(self._parent_index(start), 0)
        stop = self._normalize_index(self._parent_index(stop), len(self))
        return FWFRegion(self.parent, start, stop, self.fields)


    def iter_lines(self) -> Iterator[memoryview]:
        for i in range(self.start, self.stop):
            yield self.get_parent().raw_line_at(i)
