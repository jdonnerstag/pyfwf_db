#!/usr/bin/env python
# encoding: utf-8

"""A multi-file FWFFile

Data (files) might be provided daily, but all files of a month,
may make up the complete data set. Hence all the files must be
treated as one. Rather then physically merging the files,
FWFMultiFile allows to virtually merged them.

Both FWFFile and FWFMultiFile implement FWFViewLike, which is
what all the index implementations (base class: FWFIndexLike)
depend on.
"""

from typing import Iterator

from .fwf_view_like import FWFViewLike
from .fwf_subset import FWFSubset
from .fwf_region import FWFRegion
from .fwf_file import FWFFile


class FWFMultiFile(FWFViewLike):
    """Create a view over multiple files and allow them to be treated
    as one file.

    Regularly we receive new files every day, but must process all files
    from a past period (multiple days). With multi-file support it is not
    necessary to concat the files first.
    """

    def __init__(self, filespec):
        super().__init__(None)

        self.filespec = filespec
        self.files: list[FWFViewLike] = []

        self.line_count = 0


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


    def open_and_add(self, file) -> FWFFile:
        """Open a file complying to the filespec provided in the
        constructor, and register the file for auto-close"""

        fwf = FWFFile(self.filespec)
        fwf.open(file)     # pylint: disable=invalid-name
        self.add_file(fwf)

        return fwf


    def add_file(self, view_like: FWFFile):
        """Append a new file or view to the end"""

        assert view_like is not None

        self.files.append(view_like)

        # Update the overall line count
        self.line_count = (sum(len(x) for x in self.files))

        if self.fields is None:
            self.fields = view_like.fields


    def remove_file(self, fwf_view: FWFFile):
        """Remove a file"""
        if fwf_view is None:
            return

        self.files.remove(fwf_view)

        # Update the overall line count
        self.line_count = sum(len(x) for x in self.files)


    def determine_fwf_views(self, start: int, stop: int) -> list[FWFViewLike]:
        """Based on the start and stop indexes provided, determine which
        lines from which views are included
        """
        rtn: list[FWFViewLike] = []
        start_pos = 0
        for file in self.files:
            flen = len(file)
            ffrom = start_pos
            fto = ffrom + flen

            view = None
            if (start >= ffrom) and (stop <= fto):
                view = file[start - ffrom : stop - ffrom]
            elif ffrom <= start < fto < stop:
                view = file[start - ffrom : flen]
            elif start < ffrom <= stop <= fto:
                view = file[0 : stop - ffrom]
            elif (start < ffrom) and (stop > fto):
                view = file[:]

            if view is not None:
                rtn.append(view)

            if fto >= stop:
                break

            start_pos = fto

        return rtn


    def determine_fwf_table_index(self, index: int) -> tuple[int, int]:
        """Translate the index provided into the file and index
        within the file required to access the line.
        """
        start_pos = 0
        for i, file in enumerate(self.files):
            flen = len(file)
            ffrom = start_pos
            fto = ffrom + flen

            if ffrom <= index < fto:
                return (i, index - ffrom)

            start_pos = fto

        raise IndexError(f"Index not found: {index}")


    def __len__(self) -> int:
        return self.line_count


    def get_parent(self) -> None:
        return None


    def _parent_index(self, index: int) -> int:
        return index


    def _raw_line_at(self, index: int) -> bytes:
        idx, start = self.determine_fwf_table_index(index)
        return self.files[idx].raw_line_at(start)


    def _fwf_by_indices(self, indices: list[int]) -> FWFSubset:
        return FWFSubset(self, indices, self.get_fields())


    def _fwf_by_slice(self, start: int, stop: int) -> FWFRegion:
        return FWFRegion(self, start, stop, self.get_fields())


    def iter_lines(self) -> Iterator[bytes]:
        count = 0
        for file in self.files:
            for rec in file.iter_lines():
                yield rec
                count += 1


    def root(self, index: int) -> tuple['FWFViewLike', int]:
        idx, start = self.determine_fwf_table_index(index)
        return self.files[idx], start
