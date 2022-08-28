#!/usr/bin/env python
# encoding: utf-8

from .fwf_view_like import FWFViewLike
from .fwf_index_like import FWFIndexLike
from .fwf_line import FWFLine


class FWFMultiSubset(FWFViewLike):

    def __init__(self, fwfviews: list[FWFViewLike], lines):
        self.fwfviews = fwfviews

        # Lines is a list of tuples holding the index of the fwfview
        # and the index within that view.
        super().__init__(lines, "dummy", None)


    def __len__(self) -> int:
        """Get the number of indices (== rows) in the view"""
        return len(self.lines)


    def line_at(self, index) -> bytes:
        """Get the raw line data for the line with the index"""
        pos, index = self.lines[index]
        fwfview = self.fwfviews[pos].fwfview
        return fwfview.line_at(index)


    def fwf_by_indices(self, indices) -> 'FWFMultiSubset':
        """Create a view based on the indices provided."""
        lines = [self.lines[i] for i in indices]
        return FWFMultiSubset(self.fwfviews, lines)


    def fwf_by_slice(self, arg) -> 'FWFMultiSubset':
        """Create a view based on the slice provided."""
        lines = self.lines[arg]
        return FWFMultiSubset(self.fwfviews, lines)


    def fwf_by_line(self, idx, line) -> FWFLine:
        """Create a line based on the index and raw line data provided."""
        pos, idx = self.lines(idx)
        fwfview = self.fwfviews[pos]
        return FWFLine(fwfview, idx, fwfview.line_at[idx])


    def __iter__(self):
        for pos, idx in self.lines:
            fwfview = self.fwfviews[pos]
            line = fwfview[idx]
            yield line


    def iter_lines(self):
        """Iterate over all lines in the view, returning raw line data"""

        for i, idx in enumerate(self.lines):
            pos, idx = idx
            fwfview = self.fwfviews[pos]
            line = fwfview.line_at(idx)
            yield i, line
