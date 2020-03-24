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


class FWFIndexView(FWFViewMixin):
    """   """

    def __init__(self, parent, lines: list, columns: dict=None):

        assert parent is not None
        # assert parent.lines is not None
        # assert parent.columns is not None
        assert parent.parent is not None

        self.parent = parent.parent
        self.columns = parent.columns   # It a bit irritating. This is a dict

        # whereas the argument columns is a list
        if columns:
            self.columns = {k: v for k, v in self.columns.items() if k in columns}

        assert isinstance(lines, list)
        self.lines = lines


    def __len__(self):
        return len(self.lines)


    def line_at(self, index):
        """Get the raw line data for the line with the index"""
        return self.parent.line_at(self.lines[index])


    def __getitem__(self, args):
        """Provide support for [..] access: slice by row and column
        
        Examples:
            fwf[0]
            fwf[0:5]
            fwf[-1]
            fwf[-5:]
            fwf[:]
            fwf[0, "col-1"]
            fwf[0, ["col-1", "col-2"]]
        """

        (row_idx, cols) = args if isinstance(args, tuple) else (args, None)
        cols = cols or self.columns

        if isinstance(cols, str):
            cols = [cols]

        if isinstance(row_idx, int):
            return self.get(row_idx)
        elif isinstance(row_idx, slice):
            return FWFIndexView(self, self.lines[row_idx], cols)
        else:
            raise Exception(f"Invalid range value: {row_idx}")


    def iloc(self, start, end=None, columns=None):
        xslice = self.normalize_slice(len(self), slice(start, end))
        lines = self.lines[xslice]
        return FWFIndexView(self, lines, columns = self.columns)


    def iter_lines(self):
        for i in self.lines:
            line = self.parent.line_at(i)
            yield i, line
