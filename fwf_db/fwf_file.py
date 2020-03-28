#!/usr/bin/env python
# encoding: utf-8

import os
import collections
import mmap
import time 
from contextlib import contextmanager

from .fwf_view_like import FWFViewLike
from .fwf_line import FWFLine
from .fwf_region import FWFRegion
from .fwf_subset import FWFSubset


class FWFFile(FWFViewLike):
    """A wrapper around fixed-width files.

    This is one of the core classes. It wraps around a fixed-width file, or a 
    fixed width data block in memory, and provides access to the lines in 
    different ways.
    """

    def __init__(self, reader):
        """Constructor
        
        'reader' is a class that provides information about the 
        fixed-width file. At a minimum the class must provide the
        following properties: FIELDSPECS.

        ENCODING is an optional property to convert binary data into string.

        Please the unit tests for an example.
        """

        self.reader = reader

        # Used when automatically decoding bytes into strings
        self.encoding = getattr(reader, "ENCODING", None)   
        self.fieldspecs = [v.copy() for v in reader.FIELDSPECS]     # fixed width file spec
        self.add_slice_info(self.fieldspecs)    # The slice for each field
        self.fields = {x["name"] : x["slice"] for x in self.fieldspecs}

        self.newline_bytes = [0, 1, 10, 13]  # These bytes we recognize as newline
        self.comment_char = '#'

        # The number of newline bytes, e.g. "\r\n", "\n" or "\01"...
        # Required to determine overall line length
        self.number_of_newline_bytes = None
        self.fwidth = None      # The length of each line including newline
        self.fsize = None       # File size (including possibly missing last newline)
        self.reclen = None      # Number of records in the file
        self.start_pos = None   # Position of first record, after skipping leading comment lines
        self.lines = None       # slice(0, reclen)

        self.file = None        # File name
        self.fd = None          # open file handle
        self.mm = None          # memory map (read-only)


    def add_slice_info(self, fieldspecs):
        """Based on the field width information, determine the slice for each field"""

        startpos = 0
        for entry in fieldspecs:
            if ("len" not in entry) and ("slice" not in entry):
                raise Exception(
                    f"Fieldspecs is missing either 'len' or 'slice': {entry}")

            if ("len" in entry) and ("slice" in entry):
                continue
                #raise Exception(
                #    f"Only one must be present in a fieldspec, len or slice: {entry}")

            if "len" in entry:
                flen = entry["len"]
                if (flen <= 0) or (flen > 10000):
                    raise Exception(
                        f"Fieldspec: Invalid value for len: {entry}")

                entry["slice"] = slice(startpos, startpos + flen)
                startpos += flen
            else:
                fslice = entry["slice"]
                startpos = fslice.stop


    def is_newline(self, byte):
        return byte in self.newline_bytes


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
        """Determine the file size. 
        
        Adjust the file size if the last line has no newline.
        """

        fsize = len(mm)
        if self.is_newline(mm[fsize - 1]):
            return fsize
        
        return fsize + self.number_of_newline_bytes


    def _number_of_newline_bytes(self, mm):
        """Determine the number of newline bytes"""

        maxlen = min(len(mm), 10000)
        pos = 0
        while pos < maxlen:
            if self.is_newline(mm[pos]):
                pos += 1
                rtn = 2 if self.is_newline(mm[pos]) else 1
                return rtn

            pos += 1

        raise Exception(f"Failed to find newlines in date")

    
    def record_length(self, fields):
        return max(x.stop for x in fields.values())


    @contextmanager
    def open(self, file):
        """Initialize the fwf table with a file"""        

        if isinstance(file, str):
            self.file = file
            fd = self.fd = open(file, "rb")
            self.mm = mmap.mmap(fd.fileno(), 0, access=mmap.ACCESS_COPY)
        else:
            # Support data already loaded in whatever way. Nice for testing.
            self.file = id(file)
            self.mm = file

        self.number_of_newline_bytes = self._number_of_newline_bytes(self.mm)
        # The length of each line
        self.fwidth = self.record_length(self.fields) + self.number_of_newline_bytes   
        self.fsize = self.get_file_size(self.mm)
        self.start_pos = self.skip_comment_line(self.mm, self.comment_char)
        self.reclen = int((self.fsize - self.start_pos + 1) / self.fwidth)

        self.init_view_like(slice(0, self.reclen), self.fields)

        yield self

        self.close()


    def close(self):
        """Close the file and all open handles"""

        if self.fd:
            if self.mm:
                self.mm.close()

            self.fd.close()

        self.mm = self.fd = None


    def __len__(self):
        """Return the number of records in the file"""
        return self.lines.stop - self.lines.start

    
    def pos_from_index(self, index):
        if index < 0:
            index = len(self) + index

        assert index >= 0, f"Invalid index: {index}"

        pos = self.start_pos + (index * self.fwidth)
        if pos <= self.fsize:
            return pos

        raise Exception(f"Invalid index: {index}")

    
    def line_at(self, index):
        """Get the raw line data for the line with the index"""
        pos = self.pos_from_index(index)
        return self.mm[pos : pos + self.fwidth]


    def fwf_by_indices(self, indices):
        """Instantiate a new view based on the indices provided"""
        return FWFSubset(self, indices, self.fields)


    def fwf_by_slice(self, arg):
        """Instantiate a new view on the slice provided"""
        return FWFRegion(self, arg, self.fields)


    def fwf_by_line(self, idx, line):
        """instantiate a new FWFLine on the index and line data provided"""
        return FWFLine(self, idx, line)


    def iter_lines(self):
        """Iterate over all the lines in the file"""

        start_pos = self.start_pos
        end_pos = self.fsize
        fwidth = self.fwidth
        irow = 0
        while start_pos < end_pos:
            end = start_pos + fwidth
            # rtn = memoryview(self.mm[start_pos : end])  # It is not getting faster
            rtn = self.mm[start_pos : end]  # This is where python copies the memory
            yield irow, rtn
            start_pos = end
            irow += 1


    def iter_lines_with_field(self, field):
        """An optimized version that iterates over a single field in all lines.
        This is useful for unique and index.
        """

        fslice = self.fields[field]
        flen = fslice.stop - fslice.start
        start_pos = self.start_pos + fslice.start
        end_pos = self.fsize
        fwidth = self.fwidth
        for irow, start_pos in enumerate(range(start_pos, end_pos, fwidth)):
            rtn = self.mm[start_pos : start_pos + flen]  # This is where python copies the memory
            yield irow, rtn
