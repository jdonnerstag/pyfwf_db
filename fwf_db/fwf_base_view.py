#!/usr/bin/env python
# encoding: utf-8

"""
"""

from abc import ABC, abstractmethod
from itertools import islice


class FWFBaseView(ABC):
    """   """

    def __init__(self, parent, lines, columns):

        self.parent = parent

        if isinstance(columns, str):
            columns = [columns]

        if (columns is None) and (parent is not None):
            columns = parent.columns

        self.columns = columns

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


    @abstractmethod
    def __len__(self):
        pass


    @abstractmethod
    def iloc(self, start, end=None, columns=None):
        if columns:
            columns = [name for name in columns if name in self.columns]
        else:
            columns = self.columns

        if end is None:
            end = start + 1

        xslice = self.normalize_slice(len(self), slice(start, end))            

        return (columns, xslice)


    def __getitem__(self, args):
        (row_idx, cols) = args if isinstance(args, tuple) else (args, None)

        if isinstance(row_idx, slice):
            return self.iloc(row_idx.start, row_idx.stop, cols)
        elif isinstance(row_idx, int):
            return self.iloc(row_idx, row_idx + 1, cols)


    def __iter__(self):
        return self.iter()


    def iter(self):
        for i, line in self.iter_lines():
            rtn = [line[v] for v in self.columns.values()]
            yield (i, rtn)


    @abstractmethod
    def iter_lines(self):
        pass


    def get_raw_value(self, line, field):
        xslice = self.columns(field)
        return line[xslice]


    def get_value(self, line, field):
        return self.get_raw_value(line, field).to_bytes()


    def filter_by_line(self, func, columns=None):
        rtn = [i for i, rec in self.iter_lines() if func(rec)]

        from .fwf_index_view import FWFIndexView
        return FWFIndexView(self.parent, rtn, columns)


    def filter_by_field(self, field, func, columns=None):
        sslice = self.columns[field]

        if callable(func):
            rtn = [i for i, rec in self.iter_lines() if func(rec[sslice])]
        else:
            rtn = [i for i, rec in self.iter_lines() if rec[sslice] == func]

        from .fwf_index_view import FWFIndexView
        return FWFIndexView(self.parent, rtn, columns)


    def unique(self, field, func=None):
        sslice = self.columns[field]
        values = set()
        for _, line in self.iter_lines():
            value = line[sslice].tobytes()
            if func:
                value = func(value)
            values.add(value)

        return values


    def to_pandas(self):
        raise Exception("Not yet implemented")
