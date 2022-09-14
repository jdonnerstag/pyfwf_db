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

from typing import Iterator, Optional

from .fwf_fieldspecs import FWFFileFieldSpecs
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

    def __init__(self, reader):
        super().__init__(self._determine_fieldspecs(reader))

        self.reader = reader
        self.files: list[FWFFile] = []

        self.line_count = 0


    @classmethod
    def _determine_fieldspecs(cls, reader):
        spec = None
        try:
            spec = FWFFileFieldSpecs(reader.FIELDSPECS)
        except:     # pylint: disable=bare-except
            try:
                spec = reader.fieldspecs
            except:     # pylint: disable=bare-except
                if isinstance(reader, FWFFileFieldSpecs):
                    spec = reader

        assert spec is not None
        assert isinstance(spec, FWFFileFieldSpecs)

        return spec


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

        fwf = FWFFile(self.reader)
        fwf.open(file)
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


    def _determine_fwfview_index(self, index: int) -> tuple[int, int]:
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
        idx, start = self._determine_fwfview_index(index)
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


    def root(self, index: int, stop_view: Optional['FWFViewLike'] = None) -> tuple['FWFViewLike', int]:
        if (stop_view is not None) and (self == stop_view):
            return self, index

        idx, start = self._determine_fwfview_index(index)
        return self.files[idx], start
