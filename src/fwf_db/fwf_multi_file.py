#!/usr/bin/env python
# encoding: utf-8

from typing import TextIO
from .fwf_view_like import FWFViewLike
from .fwf_file import FWFFile
from .fwf_line import FWFLine
from .fwf_region import FWFRegion
from .fwf_subset import FWFSubset


class FWFMultiFileMixin:
    """Mix-in for common multi-file functionalities"""

    def __init__(self, filespec):
        self.filespec = filespec
        self.files: list[FWFViewLike] = []


    def __enter__(self):
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def close(self):
        """Close all open files previously registered"""
        for file in self.files:
            close = getattr(file, "close")
            if callable(close):
                close()



class FWFMultiFile(FWFMultiFileMixin, FWFViewLike):
    """Create a view over multiple files and allow them to be treated
    as one file.

    Regularly we receive new files every day, but must process all files
    from a past period (multiple days). With multi-file support it is not
    necessary to concat the files first.
    """

    def __init__(self, filespec=None):
        FWFMultiFileMixin.__init__(self, filespec)
        FWFViewLike.__init__(self, None, None, None)


    def open(self, file):
        """Open a file complying to the filespec provided in the
        constructor, and register the file for auto-close"""

        fwf = FWFFile(self.filespec)
        fd = fwf.open(file)     # pylint: disable=invalid-name

        return self.add_file(fd)


    def add_file(self, fwf_view_like):
        """Append a new file or view to the end"""

        assert fwf_view_like is not None

        self.files.append(fwf_view_like)

        # Update the overall line count
        self.lines = slice(0, sum(len(x) for x in self.files))

        # The first file added provide the fieldspecs etc.
        if self.fields is None:
            self.fields = fwf_view_like.fields

        return fwf_view_like


    def remove_file(self, fwf_view):
        """Remove a file"""
        if fwf_view is None:
            return

        self.files.remove(fwf_view)

        # Update the overall line count
        self.lines = slice(0, sum(len(x) for x in self.files))


    def determine_fwf_views(self, start, stop):
        """Based on the start and stop indexes provided, determine which
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

            if (index >= ffrom) and (index < fto):
                return (i, index - ffrom, index - ffrom + 1)

            start_pos = fto

        raise IndexError(f"Index not found: {index}")


    def __len__(self) -> int:
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


    def fwf_by_indices(self, indices) -> FWFSubset:
        """Create a new view based on the indices provided"""
        # TODO has this been tested already?
        return FWFSubset(self, indices, self.fields)


    def fwf_by_slice(self, arg) -> FWFRegion:
        """Create a new view based on the slice provided"""
        # TODO has this been tested already?
        return FWFRegion(self, arg, self.fields)


    def fwf_by_line(self, idx, line) -> FWFLine:
        """Create a new Line based on the index and raw line data provided"""
        # TODO has this been tested already?
        return FWFLine(self, idx, line)
