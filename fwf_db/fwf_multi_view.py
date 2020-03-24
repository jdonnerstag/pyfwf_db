#!/usr/bin/env python
# encoding: utf-8

"""
"""

from .fwf_view_mixin import FWFViewMixin
from .fwf_slice_view import FWFSliceView


class FWFMultiView(FWFViewMixin):
    """    """

    def __init__(self, columns=None):

        self.parent = self
        self.columns = columns
        self.files = []
        self.lines = None


    def add_file(self, fwf_view):
        assert fwf_view is not None

        self.files.append(fwf_view)
        self.lines = slice(0, sum(len(x) for x in self.files))

        if self.columns is None:
            self.columns = fwf_view.columns


    def remove_file(self, fwf_view):
        if fwf_view is None:
            return 

        self.files = self.files.remove(fwf_view)
        self.lines = slice(0, sum(len(x) for x in self.files))


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


    def iloc(self, start, stop=None, columns=None):
        rtn = FWFMultiView(self.columns)
        for view in self.determine_fwf_views(start, stop):
            rtn.add_file(view)

        return rtn
