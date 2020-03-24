#!/usr/bin/env python
# encoding: utf-8

"""Provide a view on the fixed width data. 

A view can be described as a slice of rows, or alternatively by an index.
The index value is the actual line number.

Additionally it is possible to restrict the columns available in the view.

Views can be derived from other views.
"""

from itertools import islice


class FWFView(object):
    """A potentially limited view on the underlying fixed-width file."""

    def __init__(self, parent, lines, columns):
        assert parent is not None
        assert parent.lines is not None
        assert parent.columns is not None

        self.parent = parent

        if isinstance(columns, str):
            columns = [columns]

        self.columns = columns or parent.columns

        if isinstance(lines, int):
            lines = self.normalize_slice(len(parent), slice(lines, lines + 1))

        if isinstance(lines, slice):
            lines = self.add_slices(self.parent.lines, lines)

        self.lines = lines


    def add_slices(self, a, b):
        parent_size = a.stop - a.start
        b = self.normalize_slice(parent_size, b)
        b = slice(a.start + b.start, a.start + b.stop)  
        b = self.normalize_slice(parent_size, b)
        return b


    def normalize_slice(self, parent_size, xslice):
        start = xslice.start
        stop = xslice.stop

        if start is None:
            start = 0
        elif start < 0:
            start = parent_size + start
            if stop == 0:
                stop = None	# == end of file
        
        if (start < 0) or (start >= parent_size):
            raise Exception(
                f"Invalid start index {start} for slice {xslice}. "
                f"Parent size: {parent_size}")

        if stop is None:
            stop = parent_size
        elif stop < 0:
            stop = parent_size + stop
        
        if (stop < 0) or (stop > parent_size):
            raise Exception(
                f"Invalid stop index {stop} for slice {xslice}. "
                f"Parent size: {parent_size}")

        if stop < start:
            raise Exception(
                f"Invalid slice: start <= stop: {start} <= {stop} for "
                f"slice {xslice} and parent size {parent_size}")

        return slice(start, stop)


    def __len__(self):
        l = self.lines
        if isinstance(l, list):
            return len(l)
        elif isinstance(l, slice):
            return l.stop - l.start
        else:
            raise Exception(f"Unsupported type for 'lines': {type(self.lines)}")


    def iloc(self, start, end=None, columns=None):
        if columns:
            columns = [name for name in columns if name in self.columns]
        else:
            columns = self.columns

        if end is None:
            end = start + 1

        xslice = self.normalize_slice(len(self), slice(start, end))            
        if isinstance(self.lines, slice):
            lines = self.add_slices(self.lines, xslice)
        else:
            lines = self.lines[xslice]

        if not lines:
            return None

        return FWFView(self.parent, lines, columns)


    def __getitem__(self, args):
        (row_idx, cols) = args if isinstance(args, tuple) else (args, None)

        if isinstance(row_idx, slice):
            return self.iloc(row_idx.start, row_idx.stop, cols)
        elif isinstance(row_idx, int):
            return self.iloc(row_idx, None, cols)


    def __iter__(self):
        return self.iter()


    def iter(self):
        for i, line in self.iter_lines():
            rtn = [line[v] for v in self.columns.values()]
            yield (i, rtn)


    def iter_lines(self):
        yield from  self.parent.iter_lines(self.lines)


    def get_raw_value(self, line, field):
        return self.parent.get_raw_value(line, field)


    def get_value(self, line, field):
        return self.parent.get_value(line, field)


    def filter_by_line(self, func):
        return self.parent.filter_by_line(func, self.lines, self.columns)


    def filter_by_field(self, field, func):
        return self.parent.filter_by_field(field, func, self.lines, self.columns)
        

    def unique(self, field, func=None):
        return self.parent.unique(field, func, self.lines, self.columns)


    def to_pandas(self):
        raise Exception("Not yet implemented")
