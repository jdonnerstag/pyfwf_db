#!/usr/bin/env python
# encoding: utf-8


from fwf_dbv2.fwf_view_like import FWFViewLike
from fwf_dbv2.fwf_line import FWFLine
from fwf_dbv2.fwf_subset import FWFSubset


class FWFRegion(FWFViewLike):

    def __init__(self, fwffile, lines, fields):
        assert fwffile
        self.fwffile = fwffile

        self.init_view_like(lines, fields)


    def __len__(self):
        return self.lines.stop - self.lines.start


    def line_at(self, index):
        """Get the raw line data for the line with the index"""
        return self.fwffile.line_at(self.lines.start + index)


    def fwf_by_index(self, arg):
        """Initiate a FWFRegion (or similar) object and return it"""
        lines = [self.lines.start + i for i in arg]
        return FWFSubset(self.fwffile, lines, self.fields)


    def fwf_by_slice(self, arg):
        """Initiate a FWFRegion (or similar) object and return it"""
        lines = slice(self.lines.start + arg.start, self.lines.start + arg.stop)
        return FWFRegion(self.fwffile, lines, self.fields)


    def fwf_by_line(self, idx, line):
        """Initiate a FWFRegion (or similar) object and return it"""
        return FWFLine(self.fwffile, idx, line)


    def iter_lines(self):
        """Iterate over all lines in the file, returning raw line data"""

        for i in range(self.lines.start, self.lines.stop):
            line = self.fwffile.line_at(i)
            yield i, line
