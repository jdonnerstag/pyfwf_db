#!/usr/bin/env python
# encoding: utf-8

from fwf_dbv2.fwf_view_like import FWFViewLike
from fwf_dbv2.fwf_line import FWFLine
from fwf_dbv2.fwf_region import FWFRegion
from fwf_dbv2.fwf_subset import FWFSubset


class FWFMultiFile(FWFViewLike):

    def __init__(self):

        self.files = []
        self.lines = None
        self.fields = None     # Dict(field name -> slice)


    def add_file(self, fwf_view_like):
        assert fwf_view_like is not None

        self.files.append(fwf_view_like)
        self.lines = slice(0, sum(len(x) for x in self.files))

        if self.fields is None:
            self.fields = fwf_view_like.fields

        self.init_view_like(self.lines, self.fields)


    def remove_file(self, fwf_view):
        if fwf_view is None:
            return 

        self.files = self.files.remove(fwf_view)
        self.lines = slice(0, sum(len(x) for x in self.files))

        self.init_view_like(self.lines, self.fields)


    def determine_fwf_views(self, start, stop):
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
        start_pos = 0
        for i, file in enumerate(self.files):
            flen = len(file)
            ffrom = start_pos
            fto = ffrom + flen

            if (index >= ffrom) and (index <= fto):
                return (i, index - ffrom, index - ffrom + 1)

            start_pos = fto


    def __len__(self):
        return self.lines.stop - self.lines.start


    def line_at(self, index):
        """Get the raw line data for the line with the index"""
        
        (idx, start, _) = self.determine_fwf_table_index(index)
        return self.files[idx].line_at(start)


    def iter_lines(self):
        """Iterate over all lines in the file, returning raw line data"""
        count = 0
        for file in self.files:
            for _, rec in file.iter_lines():
                yield count, rec
                count += 1


    def fwf_by_index(self, arg):
        """Initiate a FWFRegion (or similar) object and return it"""
        lines = [self.lines[i] for i in arg]
        return FWFSubset(self, lines, self.fields)


    def fwf_by_slice(self, arg):
        """Initiate a FWFRegion (or similar) object and return it"""
        return FWFRegion(self, arg, self.fields)


    def fwf_by_line(self, idx, line):
        """Initiate a FWFRegion (or similar) object and return it"""
        return FWFLine(self, idx, line)
