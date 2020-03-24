#!/usr/bin/env python
# encoding: utf-8

"""Provide a view on the fixed width data. 

A view can be described as a slice of rows, or alternatively by an index.
The index value is the actual line number.

Additionally it is possible to restrict the columns available in the view.

Views can be derived from other views.
"""

from itertools import islice

from .fwf_base_view import FWFBaseView


class FWFSliceView(FWFBaseView):

    def __init__(self, parent, lines, columns):
        super().__init__(parent, lines, columns)

        assert parent is not None
        assert parent.lines is not None
        assert parent.columns is not None

        if isinstance(lines, int):
            lines = self.normalize_slice(len(parent), slice(lines, lines + 1))

        if isinstance(lines, slice):
            lines = self.add_slices(self.parent.lines, lines)

        self.lines = lines


    def __len__(self):
        return self.lines.stop - self.lines.start


    def iloc(self, start, end=None, columns=None):
        (columns, xslice) = super().iloc(start, end, columns)
        lines = self.add_slices(self.lines, xslice)
        return FWFSliceView(self.parent, lines, columns)


    def pos_from_index(self, index):
        return self.parent.pos_from_index(index)


    def iter_lines(self):
        yield from self.parent.iter_lines_with_slice(self.lines)
