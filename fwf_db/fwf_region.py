#!/usr/bin/env python
# encoding: utf-8


from .fwf_view_like import FWFViewLike
from .fwf_line import FWFLine
from .fwf_subset import FWFSubset


class FWFRegion(FWFViewLike):
    """A view on the parents data based on a slice with start 
    and stop indexes
    """

    def __init__(self, fwffile, lines, fields):
        assert fwffile
        self.fwffile = fwffile

        self.init_view_like(lines, fields)


    def __len__(self):
        return self.lines.stop - self.lines.start


    def line_at(self, index):
        """Get the raw line data for the line with the index"""
        return self.fwffile.line_at(self.lines.start + index)


    def fwf_by_indices(self, indices):
        """Create a new view based on the indices provided"""
        lines = [self.lines.start + i for i in indices]
        return FWFSubset(self.fwffile, lines, self.fields)


    def fwf_by_slice(self, arg):
        """Create a new view based on the slice provided"""
        lines = slice(self.lines.start + arg.start, self.lines.start + arg.stop)
        return FWFRegion(self.fwffile, lines, self.fields)


    def fwf_by_line(self, idx, line):
        """Create a new Line based on the index and raw line provided"""
        return FWFLine(self.fwffile, idx, line)


    def iter_lines(self):
        """Iterate over all lines in the view, returning raw line data"""

        for i in range(self.lines.start, self.lines.stop):
            line = self.fwffile.line_at(i)
            yield i, line
