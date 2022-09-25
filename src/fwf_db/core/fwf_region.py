#!/usr/bin/env python
# encoding: utf-8

"""Define a view that represents a region of its parent view"""

from typing import Iterator, TYPE_CHECKING

from .fwf_view_like import FWFViewLike

# To prevent circular dependencies only during type checking
if TYPE_CHECKING:
    from .fwf_subset import FWFSubset


class FWFRegion(FWFViewLike):
    """A view on the parents data based on a slice with start
    and stop indexes
    """

    def __init__(self, parent: FWFViewLike, start: int, stop: int):
        super().__init__(None, parent)

        assert start >= 0
        assert start <= stop

        self.start = start
        self.stop = stop


    def count(self) -> int:
        return self.stop - self.start


    def _parent_index(self, index: int) -> int:
        return self.start + index


    def _raw_line_at(self, index: int) -> memoryview:
        assert self.parent is not None
        index = self.parent_index(index)
        return self.parent.raw_line_at(index)


    def _fwf_by_indices(self, indices: list[int]) -> 'FWFSubset':
        from .fwf_subset import FWFSubset   # pylint: disable=import-outside-toplevel
        return FWFSubset(self, indices)


    def _fwf_by_slice(self, start: int, stop: int) -> 'FWFRegion':
        return FWFRegion(self, start, stop)


    def iter_lines(self) -> Iterator[memoryview]:
        assert self.parent is not None
        for i in range(self.start, self.stop):
            yield self.parent.raw_line_at(i)
