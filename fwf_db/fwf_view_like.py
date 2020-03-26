#!/usr/bin/env python
# encoding: utf-8

import abc 


class FWFViewLike(abc.ABC):
    """A core class. Provide all the necessary basics to implement different
    kind of views, such as views based on a slice, or views based on 
    indivisual indexes.
    """

    def init_view_like(self, lines, fields):
        assert lines is not None
        assert fields is not None

        self.lines = lines
        self.fields = fields


    @abc.abstractmethod
    def __len__(self):
        """Varies depending on the view implementation"""


    @abc.abstractmethod
    def line_at(self, index):
        """Get the raw line data for the line with the index"""


    @abc.abstractmethod
    def fwf_by_indices(self, indices):
        """Initiate a FWFLine (or similar) object and return it"""


    @abc.abstractmethod
    def fwf_by_slice(self, arg):
        """Initiate a FWFRegion (or similar) object and return it"""


    @abc.abstractmethod
    def fwf_by_line(self, idx, line):
        """Initiate a FWFRegion (or similar) object and return it"""


    def normalize_index(self, index, default, stop):
        """For start and stop values of a slice, determine sensible
        default when the index is None or < 0
        """
        if index is None:
            index = default
        elif index < 0:
            index = len(self) + index + stop
        return index


    def __getitem__(self, row_idx):
        """Provide support for [..] access: slice by row and column
        
        Examples:
            fwf[0]
            fwf[0:5]
            fwf[-1]
            fwf[-5:]
            fwf[:]
        """

        if isinstance(row_idx, int):
            row_idx = self.normalize_index(row_idx, 0, 0)
            return self.fwf_by_line(row_idx, self.line_at(row_idx))
        elif isinstance(row_idx, slice):
            start = self.normalize_index(row_idx.start, 0, 0)
            stop = self.normalize_index(row_idx.stop, len(self), 1)
            return self.fwf_by_slice(slice(start, stop))
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
            rtn = self.fwf_by_line(i, line)
            yield rtn


    @abc.abstractmethod
    def iter_lines(self):
        """Iterate over all lines in the file, returning raw line data"""

        return (None, None)     # (Index, line)


    def filter_by_line(self, func):
        """Filter lines with a condition
        
        Iterate over all lines in the file and apply 'func' to every line. Except
        if 'func' returns True, the line will be skipped.

        The result is a view on the data, rather then copies.
        """

        rtn = [i for i, rec in self.iter_lines() if func(self.fwf_by_line(i, rec))]
        return self.fwf_by_indices(rtn)


    def filter_by_field(self, field, func):
        """Filter lines by the 'field' and 'func' provided.
        
        Iterate over all lines in the file, determine the (byte) value for the field, 
        and apply 'func' to that field value. Except if 'func' returns True, the 
        line will be skipped.

        The result is a view on the data, rather then copies.
        """

        sslice = self.fields[field]

        if callable(func):
            rtn = [i for i, rec in self.iter_lines() if func(rec[sslice])]
        else:
            rtn = [i for i, rec in self.iter_lines() if rec[sslice] == func]

        return self.fwf_by_indices(rtn)


    def to_pandas(self):
        """Export the data to Pandas dataframes"""
        # TODO

        raise Exception("Not yet implemented")
