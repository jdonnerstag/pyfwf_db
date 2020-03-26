#!/usr/bin/env python
# encoding: utf-8


from .fwf_view_like import FWFViewLike
from .fwf_line import FWFLine


class FWFSubset(FWFViewLike):

    def __init__(self, fwffile, lines, fields):
        assert fwffile
        self.fwffile = fwffile

        self.init_view_like(lines, fields)


    def __len__(self):
        return len(self.lines)


    def line_at(self, index):
        """Get the raw line data for the line with the index"""
        return self.fwffile.line_at(self.lines[index])


    def fwf_by_index(self, arg):
        """Initiate a FWFRegion (or similar) object and return it"""
        lines = [self.lines[i] for i in arg]
        return FWFSubset(self.fwffile, lines, self.fields)


    def fwf_by_slice(self, arg):
        """Initiate a FWFRegion (or similar) object and return it"""
        lines = self.lines[arg]
        return FWFSubset(self.fwffile, lines, self.fields)


    def fwf_by_line(self, idx, line):
        """Initiate a FWFRegion (or similar) object and return it"""
        return FWFLine(self.fwffile, self.lines[idx], line)


    def iter_lines(self):
        """Iterate over all lines in the file, returning raw line data"""

        for i, idx in enumerate(self.lines):
            line = self.fwffile.line_at(idx)
            yield i, line
