#!/usr/bin/env python
# encoding: utf-8


from .fwf_view_like import FWFViewLike
from .fwf_line import FWFLine


class FWFSubset(FWFViewLike):
    """A view based on a list of individual indices"""

    def __init__(self, fwffile, lines, fields):
        assert fwffile is not None
        self.fwffile = fwffile

        # Lines is a list of integer holding the indices
        self.init_view_like(lines, fields)


    def __len__(self):
        """Get the number of indices (== rows) in the view"""
        return len(self.lines)


    def line_at(self, index):
        """Get the raw line data for the line with the index"""
        return self.fwffile.line_at(self.lines[index])


    def fwf_by_indices(self, indices):
        """Create a view based on the indices provided."""
        lines = [self.lines[i] for i in indices]
        return FWFSubset(self.fwffile, lines, self.fields)


    def fwf_by_slice(self, arg):
        """Create a view based on the slice provided."""
        lines = self.lines[arg]
        return FWFSubset(self.fwffile, lines, self.fields)


    def fwf_by_line(self, idx, line):
        """Create a line based on the index and raw line data provided."""
        return FWFLine(self.fwffile, self.lines[idx], line)


    def iter_lines(self):
        """Iterate over all lines in the view, returning raw line data"""

        for i, idx in enumerate(self.lines):
            line = self.fwffile.line_at(idx)
            yield i, line

    def close(self):
        self.fwffile.close()
        