#!/usr/bin/env python
# encoding: utf-8

"""Provide a view on the fixed width data. 

A view can be described as a slice of rows, or alternatively by an index.
The index value is the actual line number.

Additionally it is possible to restrict the columns available in the view.

Views can be derived from other views.
"""

from itertools import islice

from .fwf_view_mixin import FWFViewMixin


class FWFSliceView(FWFViewMixin):

    def __init__(self, parent, lines, columns=None):

        assert parent is not None
        assert parent.lines is not None
        assert parent.columns is not None
        assert parent.parent is not None

        self.parent = parent.parent
        self.columns = parent.columns   # It a bit irritating. This is a dict

        # whereas the argument columns is a list
        if columns:
            self.columns = {k: v for k, v in self.columns.items() if k in columns}

        if isinstance(lines, int):
            lines = self.normalize_slice(len(parent), slice(lines, lines + 1))

        if isinstance(lines, slice):
            lines = self.intersect_slices(parent.lines, lines)

        self.lines = lines


    def __len__(self):
        return self.lines.stop - self.lines.start


    def line_at(self, index):
        """Get the raw line data for the line with the index"""

        idx = self.lines.start + index
        if (idx >= 0) and (idx <= self.lines.start):
            return self.parent.line_at(idx)

        raise Exception(f"Invalid index: {index}")


    def iloc(self, start, end=None, columns=None):
        xslice = self.normalize_slice(len(self), slice(start, end))
        lines = self.intersect_slices(self.lines, xslice)
        return FWFSliceView(self.parent, lines, columns)


    def pos_from_index(self, index):
        return self.parent.pos_from_index(index)


    def iter_lines(self):
        yield from self.parent.iter_lines_with_slice(self.lines)
