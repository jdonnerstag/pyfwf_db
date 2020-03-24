#!/usr/bin/env python
# encoding: utf-8

"""
"""

from abc import ABC, abstractmethod

from .fwf_line import FWFLine


class FWFViewMixin(ABC):
    """   """

    def __init__(self):

        self.parent = None
        self.columns = None
        self.lines = None


    def validate_slice(self, parent_size, xslice):
        start = xslice.start
        stop = xslice.stop

        if (start < 0) or (start >= parent_size):
            raise Exception(
                f"Invalid start index {start} for slice {xslice}. "
                f"Parent size: {parent_size}")

        
        if (stop < 0) or (stop > parent_size):
            raise Exception(
                f"Invalid stop index {stop} for slice {xslice}. "
                f"Parent size: {parent_size}")

        if stop < start:
            raise Exception(
                f"Invalid slice: start <= stop: {start} <= {stop} for "
                f"slice {xslice} and parent size {parent_size}")

        return xslice


    def intersect_slices(self, a, b):
        parent_size = a.stop - a.start

        # Make sure start and stop are >= 0 and valid
        b = self.normalize_slice(parent_size, b)

        # Add the second slice to the first one 
        b = slice(a.start + b.start, a.start + b.stop)  

        # Make sure the result is still valid
        b = self.validate_slice(parent_size, b)
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
        
        if stop is None:
            stop = parent_size
        elif stop < 0:
            stop = parent_size + stop
        
        rtn = self.validate_slice(parent_size, slice(start, stop))
        return rtn


    @abstractmethod
    def __len__(self):
        pass


    @abstractmethod
    def line_at(self, index):
        """Get the raw line data for the line with the index"""


    def get(self, index, columns=None):
        """Get an object providing easy access to the data in the line 
        denoted by index"""

        if index < 0:
            index = len(self) + index

        assert index >= 0

        return FWFLine(self, index, self.line_at(index), columns)


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

        if isinstance(cols, str):
            cols = [cols]

        if isinstance(row_idx, int):
            return self.get(row_idx, cols)
        elif isinstance(row_idx, slice):
            from .fwf_slice_view import FWFSliceView
            return FWFSliceView(self, row_idx, cols)
        else:
            raise Exception(f"Invalid range value: {row_idx}")


    def __iter__(self):
        """iterate over all rows. 
        
        Return an object describing the line and providing access to 
        each field.
        """

        return self.iter()


    def iter(self):
        """iterate over all rows. 
        
        Return an object describing the line and providing access to 
        each field.
        """

        for i, line in self.iter_lines():
            rtn = FWFLine(self, i, line)
            yield rtn


    @abstractmethod
    def iter_lines(self):
        """Iterate over all lines in the file, returning raw line data"""

        pass


    def filter_by_line(self, func, columns=None):
        """Filter lines with a condition
        
        Iterate over all lines in the file and apply 'func' to every line. Except
        if 'func' returns True, the line will be skipped.

        The result is a view on the data, rather then copies.
        """

        rtn = [i for i, rec in self.iter_lines() if func(rec)]

        from .fwf_index_view import FWFIndexView
        return FWFIndexView(self, rtn, columns)


    def filter_by_field(self, field, func, columns=None):
        """Filter lines by the 'field' and 'func' provided.
        
        Iterate over all lines in the file, determine the (byte) value for the field, 
        and apply 'func' to that field value. Except if 'func' returns True, the 
        line will be skipped.

        The result is a view on the data, rather then copies.
        """

        sslice = self.columns[field]

        if callable(func):
            rtn = [i for i, rec in self.iter_lines() if func(rec[sslice])]
        else:
            rtn = [i for i, rec in self.iter_lines() if rec[sslice] == func]

        from .fwf_index_view import FWFIndexView
        return FWFIndexView(self, rtn, columns)


    def unique(self, field, func=None):
        """Create a list of unique values found in 'field'.

        Use 'func' to change the value before adding it to the index, e.g. 
        str, lower, upper, int, ...
        """

        sslice = self.parent.columns[field]
        values = set()
        for _, line in self.iter_lines():
            value = line[sslice]
            if func:
                value = func(value)
            values.add(value)

        return values


    def to_pandas(self):
        """Export the data to Pandas dataframes"""

        raise Exception("Not yet implemented")
