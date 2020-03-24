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


class FWFIndexView(FWFBaseView):
    """   """

    def __init__(self, parent, lines, columns):
        super().__init__(parent, lines, columns)

        assert parent is not None
        assert parent.lines is not None
        assert parent.columns is not None

        assert isinstance(lines, list)


    def __len__(self):
        return len(self.lines)


    def iloc(self, start, end=None, columns=None):
        (columns, xslice) = super().iloc(start, end, columns)
        lines = self.lines[xslice]
        return FWFIndexView(self.parent, lines, columns)


    def iter_lines(self):
        for i in self.lines:
            line = self.parent.line_at(i)
            yield i, line
