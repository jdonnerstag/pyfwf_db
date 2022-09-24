#!/usr/bin/env python
# encoding: utf-8

"""Define a view which is a subset of the parent view"""

from typing import Iterator

from .fwf_view_like import FWFViewLike


class FWFSubset(FWFViewLike):
    """A view based on a list of individual indices"""

    def __init__(self, parent: FWFViewLike, lines: list[int]):
        super().__init__(parent.filespec)

        self.parent = parent
        self.lines = lines


    def count(self) -> int:
        return len(self.lines)


    def get_parent(self) -> 'FWFViewLike':
        return self.parent


    def _parent_index(self, index: int) -> int:
        return self.lines[index]


    def _raw_line_at(self, index: int) -> memoryview:
        index = self._parent_index(index)
        return self.get_parent().raw_line_at(index)


    def iter_lines(self) -> Iterator[memoryview]:
        for idx in self.lines:
            yield self.get_parent().raw_line_at(idx)


    def _fwf_by_indices(self, indices: list[int]) -> 'FWFSubset':
        indices = [self.parent_index(i) for i in indices]
        return FWFSubset(self.get_parent(), indices)


    def _fwf_by_slice(self, start: int, stop: int) -> 'FWFSubset':
        # Note: we are creating a FWFSubset rather then a FWFRegion for
        # two reasons:
        # 1. We avoid one extra redirection when accessing elements
        # 2. We avoid a circular dependency between FWFSubset and FWFRegion
        lines = [self.parent_index(i) for i in range(start, stop)]
        return FWFSubset(self.get_parent(), lines)
