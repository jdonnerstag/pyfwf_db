#!/usr/bin/env python
# encoding: utf-8

"""Define a view which is a subset of the parent view"""

from typing import Iterator, TYPE_CHECKING

from .fwf_view_like import FWFViewLike

# To prevent circular dependencies only during type checking
if TYPE_CHECKING:
    from .fwf_region import FWFRegion


class FWFSubset(FWFViewLike):
    """A view based on a list of individual indices"""

    def __init__(self, parent: FWFViewLike, lines: list[int]):
        super().__init__(None, parent)

        self.lines = lines


    def count(self) -> int:
        return len(self.lines)


    def _parent_index(self, index: int) -> int:
        return self.lines[index]


    def _raw_line_at(self, index: int) -> memoryview:
        assert self.parent is not None
        index = self._parent_index(index)
        return self.parent.raw_line_at(index)


    def iter_lines(self) -> Iterator[memoryview]:
        assert self.parent is not None
        for idx in self.lines:
            # pylint: disable=reportOptionalMemberAccess
            yield self.parent.raw_line_at(idx)


    def _fwf_by_indices(self, indices: list[int]) -> 'FWFSubset':
        return FWFSubset(self, indices)


    def _fwf_by_slice(self, start: int, stop: int) -> 'FWFRegion':
        from .fwf_region import FWFRegion   # pylint: disable=import-outside-toplevel
        return FWFRegion(self, start, stop)
