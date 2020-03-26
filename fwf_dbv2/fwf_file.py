#!/usr/bin/env python
# encoding: utf-8

import os
import collections
import mmap
import time 
from contextlib import contextmanager

from fwf_dbv2.fwf_view_like import FWFViewLike
from fwf_dbv2.fwf_line import FWFLine
from fwf_dbv2.fwf_region import FWFRegion
from fwf_dbv2.fwf_subset import FWFSubset


class FWFFile(FWFViewLike):
    """A wrapper around fixed-width files.

    This is one of the core classes. It wraps around a fixed-width file, or a 
    fixed width file representation already available in memory, and provides
    access to the lines in different ways.
    """

    def __init__(self, reader):
        """Constructor
        
        'reader' is a class that provides information about the 
        fixed-width file. At a minimum the class must provide the
        following properties: ENCODING, FIELDSPECS
        """

        self.reader = reader

        # Used when automatically decoding bytes into strings
        self.encoding = getattr(reader, "ENCODING", None)   
        self.fieldspecs = reader.FIELDSPECS     # fixed width file spec

        self.widths = [x["len"] for x in self.fieldspecs]   # The width of each field
        self.add_slice_info(self.fieldspecs, self.widths)    # The slice for each field
        self.fields = {x["name"] : x["slice"] for i, x in enumerate(self.fieldspecs)}

        self.newline_bytes = [0, 1, 10, 13]  # These bytes we recognize as newline
        self.comment_char = '#'

        # The number of newline bytes, e.g. "\r\n", "\n" or "\01"...
        # Required to determine overall line length
        self.number_of_newline_bytes = None
        self.fwidth = None      # The length of each line including newline
        self.fsize = None       # File size
        self.reclen = None      # Number of records in the file
        self.start_pos = None   # Position of first record, after skipping leading comment lines
        self.lines = None       # slice(0, reclen)

        self.file = None        # File name
        self.fd = None          # open file handle
        self.mm = None          # memory map (read-only)


    def add_slice_info(self, fieldspecs, widths):
        """Based on the field width information, determine the slice for each field"""

        startpos = 0
        for entry in fieldspecs:
            flen = entry["len"]
            entry["slice"] = slice(startpos, startpos + flen)
            startpos += flen

    
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
        else:
            # Support data already loaded in whatever way. Nice for testing.
            self.file = id(file)
            self.mm = file

        self.number_of_newline_bytes = self._number_of_newline_bytes(self.mm)
        self.fwidth = sum(self.widths) + self.number_of_newline_bytes   # The length of each line
        self.fsize = self.get_file_size(self.mm)
        self.start_pos = self.skip_comment_line(self.mm, self.comment_char)
        self.reclen = int((self.fsize - self.start_pos + 1) / self.fwidth)
        self.lines = slice(0, self.reclen)

        self.init_view_like(slice(0, self.reclen), self.fields)

        yield self

        self.close()


    def close(self):
        """Close the file and all open handles"""

        # When you receive an error alluding to a pointers still holding to the 
        # memoryview, then look for your "with fwf.open().." block. A with 
        # statement does not create a scope (like if, for and while do not create 
        # a scope either), and hence variables which have FWFView or FWFIndex
        # may still hold references to memoryview. Assign "None" to the variable
        # to release the data.

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

        assert index >= 0

        pos = self.start_pos + (index * self.fwidth)
        if pos <= self.fsize:
            return pos

        raise Exception(f"Invalid index: {index}")

    
    def line_at(self, index):
        """Get the raw line data for the line with the index"""
        pos = self.pos_from_index(index)
        return self.mm[pos : pos + self.fwidth]


    def fwf_by_index(self, arg):
        """Initiate a FWFRegion (or similar) object and return it"""
        return FWFSubset(self, arg, self.fields)


    def fwf_by_slice(self, arg):
        """Initiate a FWFRegion (or similar) object and return it"""
        return FWFRegion(self, arg, self.fields)


    def fwf_by_line(self, idx, line):
        """Initiate a FWFRegion (or similar) object and return it"""
        return FWFLine(self, idx, line)


    def iter_lines(self):
        """Iterate over the lines denoted by xslice.

        Return raw lines. The start and stop positions must be positiv
        integer value and valid.

        This is mostly a helper function for View implementations.
        """

        start_pos = self.start_pos
        end_pos = self.fsize
        irow = 0
        while start_pos < end_pos:
            end = start_pos + self.fwidth
            rtn = self.mm[start_pos : end]
            yield irow, rtn
            start_pos = end
            irow += 1
