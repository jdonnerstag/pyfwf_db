#!/usr/bin/env python
# encoding: utf-8

import os
import collections
import mmap
import time 
from contextlib import contextmanager

from .fwf_view import FWFView
from .fwf_index_view import FWFIndexView
from .fwf_slice_view import FWFSliceView


class FWFTable(object):
    """FWFTable encapsulate the fixed width file"""

    def __init__(self, reader):
        """Constructor
        
        'reader' is a class that provides information about the 
        fixed-width file. At a minimum the class must provide the
        following properties: ENCODING, FIELDSPECS
        """

        self.reader = reader

        # Needed for decoding bytes into string
        self.encoding = getattr(reader, "ENCODING", None)   
        self.fieldspecs = reader.FIELDSPECS     # fixed width file spec

        self.widths = [x["len"] for x in self.fieldspecs]   # The width of each field
        self.slices = self.determine_slices(self.widths)    # The slice for each field
        self.columns = {x["name"] : self.slices[i] for i, x in enumerate(self.fieldspecs)}

        # The number of newline bytes, e.g. "\r\n", "\n" or "\01"...
        self.number_of_newline_bytes = 1
        self.fwidth = sum(self.widths) + self.number_of_newline_bytes   # The length of each line

        self.fsize = None   # File size
        self.estimated_records = None   # Estimated number of records in the file
        self.start_pos = None   # without leading comment lines
        self.lines = None       # slice(0, no-of-records)

        self.file = None
        self.fd = None
        self.mm = None
        self.mv = None


    def determine_slices(self, widths):
        """Based on the field width information, determine the slice for eac field"""
        slices = []
        startpos = 0
        for flen in widths:
            slices.append(slice(startpos, startpos + flen))
            startpos += flen

        return slices

    
    def is_newline(self, byte):
        return byte in [0, 1, 10, 13]


    def skip_comment_line(self, mm, comment_char):
        """Find the first line that is not a comment line and return its position."""
        comment_char = ord(comment_char)
        pos = 0
        next_line = (mm[pos] == comment_char)
        while next_line and (pos < 2000):
            pos += 1
            if self.is_newline(mm[pos]):
                pos += 1
                if self.is_newline(mm[pos]):
                    pos += 1

                next_line = (mm[pos] == comment_char)

        return pos


    def get_file_size(self, mm):
        fsize = len(mm)
        if self.is_newline(mm[fsize - 1]):
            return fsize
        
        return fsize + self.number_of_newline_bytes


    def determine_newline_bytes(self, mm):
        pos = 0
        while pos < len(mm):
            if self.is_newline(mm[pos]):
                pos += 1
                rtn = 2 if self.is_newline(mm[pos]) else 1
                return rtn

            pos += 1

        raise Exception(f"Failed to find newlines in date")


    @contextmanager
    def open(self, file):
        """Initialize the fwf_table with the file"""        

        if isinstance(file, str):
            self.file = file
            fd = self.fd = open(file, "rb")
            self.mm = mmap.mmap(fd.fileno(), 0, access=mmap.ACCESS_COPY)
            self.mv = memoryview(self.mm).toreadonly()
        else:
            self.file = id(file)
            self.mv = memoryview(file).toreadonly()

        self.number_of_newline_bytes = self.determine_newline_bytes(self.mv)
        self.fsize = self.get_file_size(self.mv)
        self.start_pos = self.skip_comment_line(self.mv, '#')
        self.estimated_records = int((self.fsize - self.start_pos + 1) / self.fwidth)
        self.lines = slice(0, self.estimated_records)

        cols = collections.OrderedDict()
        for i, name in enumerate(self.columns):
            cols[name] = self.slices[i]

        #logger.debug(f"File size: {self.fsize:,d} - estimated record: {self.estimated_records:,d}")

        yield self

        self.close()


    def close(self):
        """Close the file"""

        # When you receive an error alluding to a pointers still holding to the 
        # memoryview, then look for your "with fwf.open().." block. A with 
        # statement does not create a scope (like if, for and while do not create 
        # a scope either), and hence variables which have FWFView or FWFIndex
        # may still hold references to memoryview. Assign "None" to the variable
        # to release the data.

        if self.mv:
            self.mv.release()

        if self.mm:
            self.mm.close()

        if self.fd:
            self.fd.close()

        self.mv = self.mm = self.fd = None


    def __getitem__(self, args):
        """Provide [..] access to fixed width data: slice by row and column"""

        (row_idx, cols) = args if isinstance(args, tuple) else (args, None)

        if isinstance(row_idx, int):
            return FWFSliceView(self, slice(row_idx, row_idx + 1), cols)
        elif isinstance(row_idx, slice):
            return FWFSliceView(self, row_idx, cols)
        else:
            raise Exception(f"Invalid range value: {row_idx}")


    def __iter__(self):
        return self.iter(self.lines)


    def __len__(self):
        return self.lines.stop - self.lines.start


    def pos_from_index(self, index):
        assert index >= 0

        pos = self.start_pos + (index * self.fwidth)
        if pos <= len(self.mv):
            return pos

        raise Exception(f"Invalid index: too large: {index}")


    def iloc(self, index):
        pos = self.pos_from_index(index)
        return self.mv[pos : pos + self.fwidth]


    def iter_lines_with_slice(self, xslice=None):
        xslice = xslice or self.lines
        irow = xslice.start
        start_pos = self.pos_from_index(xslice.start)
        end_pos = self.pos_from_index(xslice.stop)

        while start_pos < end_pos:
            end = start_pos + self.fwidth
            rtn = self.mv[start_pos : end]
            yield irow, rtn
            start_pos = end
            irow += 1


    def iter_lines_with_index(self, indices):
        for i in indices:
            line = self.iloc(i)
            yield i, line


    def iter_lines(self, rows=None):
        if rows is None:
            rows = self.lines

        if isinstance(rows, slice):
            yield from self.iter_lines_with_slice(rows)
        elif isinstance(rows, list):
            yield from self.iter_lines_with_index(rows)
        elif isinstance(rows, int):
            yield from self.iter_lines_with_index([rows])
        else:
            raise Exception(f"Invalid range: {self.lines}")


    def iter(self, rows=None):
        for i, line in self.iter_lines(rows):
            rtn = [line[v] for v in self.columns.values()]
            yield (i, rtn)


    def iter_pos(self, start_pos, end_pos):
        irow = 0
        while start_pos < end_pos:
            end = start_pos + self.fwidth
            rtn = self.mv[start_pos : end]
            yield irow, rtn
            start_pos = end
            irow += 1


    def get_raw_value(self, line, field):
        xslice = self.columns(field)
        return line[xslice]


    def get_value(self, line, field):
        return self.get_raw_value(line, field).to_bytes()


    def filter_by_line(self, func, rows=None, columns=None):
        columns = columns or self.columns
        rtn = [i for i, rec in self.iter_lines(rows) if func(rec)]
        return FWFView(self, rtn, columns)                


    def filter_by_field(self, field, func, rows=None, columns=None):
        columns = columns or self.columns
        sslice = columns[field]

        if callable(func):
            rtn = [i for i, rec in self.iter_lines(rows) if func(rec[sslice])]
        else:
            rtn = [i for i, rec in self.iter_lines(rows) if rec[sslice] == func]

        return FWFView(self, rtn, columns)                
        

    def unique(self, field, func=None, rows=None, columns=None):
        columns = columns or self.columns
        sslice = self.columns[field]
        values = set()
        for _, line in self.iter_lines(rows):
            value = line[sslice].tobytes()
            if func:
                value = func(value)
            values.add(value)

        return values


    def to_pandas(self):
        raise Exception("Not yet implemented")
