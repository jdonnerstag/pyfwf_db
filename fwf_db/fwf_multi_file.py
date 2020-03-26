#!/usr/bin/env python
# encoding: utf-8

from .fwf_view_like import FWFViewLike
from .fwf_line import FWFLine
from .fwf_region import FWFRegion
from .fwf_subset import FWFSubset


class FWFMultiFile(FWFViewLike):
    """Create a view over multiple files and allow them to be treated
    as one file. 
    
    Regularly we receive new files every day, but must process the files
    from the a past period. With multi-file support it is not necessary
    to concat the files first.
    """

    def __init__(self):

        self.files = []        # view-like 
        self.lines = None      # slice(0, <overall number of records>)
        self.fields = None     # Dict(field name -> slice)


    def add_file(self, fwf_view_like):
        """Append a new file or view to the end"""

        assert fwf_view_like is not None

        self.files.append(fwf_view_like)

        # Update the overall line count
        self.lines = slice(0, sum(len(x) for x in self.files))

        # The first file added provide the fieldspecs etc.
        if self.fields is None:
            self.fields = fwf_view_like.fields

        # Re-initialize the view
        self.init_view_like(self.lines, self.fields)


    def remove_file(self, fwf_view):
        """Remove a file"""
        if fwf_view is None:
            return 

        self.files = self.files.remove(fwf_view)

        # Update the overall line count
        self.lines = slice(0, sum(len(x) for x in self.files))

        # Re-initialize the view
        self.init_view_like(self.lines, self.fields)


    def determine_fwf_views(self, start, stop):
        """Based in the start and stop indexes provided, determine which
        lines from which views are included
        """
        rtn = []
        start_pos = 0
        for file in self.files:
            flen = len(file)
            ffrom = start_pos
            fto = ffrom + flen

            view = None
            if (start >= ffrom) and (stop <= fto):
                view = file[start - ffrom : stop - ffrom]
            elif (start >= ffrom) and (start < fto) and (stop > fto):
                view = file[start - ffrom : flen]
            elif (start < ffrom) and (stop >= ffrom) and (stop <= fto):
                view = file[0 : stop - ffrom]
            elif (start < ffrom) and (stop > fto):
                view = file[:]

            if view is not None:
                rtn.append(view)

            if (fto >= stop):
                break

            start_pos = fto

        return rtn


    def determine_fwf_table_index(self, index):
        """Translate the index provided into the file and index 
        within the file required to access the line.
        """
        start_pos = 0
        for i, file in enumerate(self.files):
            flen = len(file)
            ffrom = start_pos
            fto = ffrom + flen

            if (index >= ffrom) and (index <= fto):
                return (i, index - ffrom, index - ffrom + 1)

            start_pos = fto


    def __len__(self):
        """Overall number of records in all files added so far"""
        return self.lines.stop - self.lines.start


    def line_at(self, index):
        """Get the raw line data for the line with the index"""
        (idx, start, _) = self.determine_fwf_table_index(index)
        return self.files[idx].line_at(start)


    def iter_lines(self):
        """Iterate over all lines in all files, returning raw line data"""
        count = 0
        for file in self.files:
            for _, rec in file.iter_lines():
                yield count, rec
                count += 1


    def fwf_by_indices(self, indices):
        """Create a new view based on the indices provided"""
        # TODO has this been tested already?
        return FWFSubset(self, indices, self.fields)


    def fwf_by_slice(self, arg):
        """Create a new view based on the slice provided"""
        # TODO has this been tested already?
        return FWFRegion(self, arg, self.fields)


    def fwf_by_line(self, idx, line):
        """Create a new Line based on the index and raw line data provided"""
        # TODO has this been tested already?
        return FWFLine(self, idx, line)
