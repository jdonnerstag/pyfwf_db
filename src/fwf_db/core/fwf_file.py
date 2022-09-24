#!/usr/bin/env python
# encoding: utf-8

""" FWFFile """

import mmap
from typing import Iterator

from .fwf_fieldspecs import FWFFileFieldSpecs
from .fwf_view_like import FWFViewLike
from .fwf_region import FWFRegion
from .fwf_subset import FWFSubset


class FWFFileException(Exception):
    """ FWFFileException """


class FWFFile(FWFViewLike):
    """A wrapper around fixed-width files.

    This is one of the core classes. It wraps around a fixed-width file, or a
    fixed width data block in memory, and provides access to the lines in
    different ways.
    """

    def __init__(self, filespec, encoding=None, newline=None, comments=None):
        """Constructor

        'reader' is a class that provides information about the
        fixed-width file. At a minimum the class must provide the
        following properties: FIELDSPECS.

        ENCODING is an optional property to convert binary data into string.
        NEWLINE is an optional property providing the list of allowed newline chars
        COMMENTS is an optional property containing the line comment char (default: #)

        Please see the unit tests for an example.
        """

        super().__init__(filespec)

        # Used when automatically decoding bytes into strings
        self.encoding = encoding or getattr(filespec, "ENCODING", None)
        self.newline_bytes = newline or getattr(filespec, "NEWLINE", [0, 1, 10, 13])  # These bytes we recognize as newline
        self.comments = comments if comments is not None else getattr(filespec, "COMMENTS", '#')

        # The number of newline bytes, e.g. "\r\n", "\n" or "\01"...
        # Required to determine overall line length
        self.number_of_newline_bytes = 0
        self.fwidth = -1      # The length of each line including newline
        self.fsize = -1       # File size (including possibly missing last newline)
        self.line_count = -1  # Number of records in the file
        self.start_pos = -1   # Position of first record, after skipping leading comment lines

        self.file = None        # File name
        self._fd = None         # open file handle
        self._mm: memoryview|None = None   # memory map (read-only)

        # This is only to be consistent with FWFMultiFile and thus avoid
        # false-positiv pylint warnings
        self.files = [self.file]


    def data(self, fslice: slice) -> bytes:
        """Some you may want to take a look at the underlying (raw) data"""
        assert self._mm is not None
        return bytes(self._mm[fslice])


    def is_newline(self, byte: int) -> bool:
        """True, if byte if one of the configured newline strings"""
        return byte in self.newline_bytes


    def skip_comment_line(self, _mm: memoryview, comments: str) -> int:
        """Find the first line that is not a comment line and return its position."""

        clen = len(comments)
        if clen == 0:
            return 0

        def skip_line(data, pos):
            while pos < len(data):
                if self.is_newline(data[pos]):
                    return pos + self.number_of_newline_bytes

                pos += 1

            return pos

        bcomment = comments.encode("utf-8")
        pos = 0
        while pos < len(_mm):
            if _mm[pos:pos + clen] != bcomment:
                break

            pos = skip_line(_mm, pos)

        return pos


    def get_file_size(self, _mm: memoryview) -> int:
        """Determine the file size.

        Adjust the file size if the last line has no newline.
        TODO I don't like that it is virtually changing the file size. It may create confusion.
        """

        fsize = len(_mm)
        if (fsize > 0) and self.is_newline(_mm[fsize - 1]):
            return fsize

        return fsize + self.number_of_newline_bytes


    def _number_of_newline_bytes(self, _mm) -> int:
        """Determine the number of newline bytes"""

        maxlen = min(len(_mm), 10000)
        pos = 0
        while pos < maxlen:
            if self.is_newline(_mm[pos]):
                pos += 1
                if pos < len(_mm):
                    return 2 if self.is_newline(_mm[pos]) else 1

                return 1

            pos += 1

        if pos == len(_mm):
            # File has only 1 line and no newline
            return 1

        raise FWFFileException("Failed to find newlines in date")


    def _next_newline(self, _mm: memoryview, start_pos: int) -> int:
        i = 0
        for i, byte in enumerate(_mm[start_pos:]):
            if self.is_newline(byte):
                return i

        return -1


    def record_length(self, _mm: memoryview, fields: FWFFileFieldSpecs, start_pos: int) -> int:
        """Determine the overall record length from the fieldspecs"""

        field_len = max(x.stop for x in fields.values()) if fields else (len(_mm) - start_pos)
        reclen = self._next_newline(_mm, start_pos)
        return reclen if reclen > field_len else field_len


    def __enter__(self):
        """Make this class a context manager. What it enables is to use
        this class and open() in a with clause, as an alternative to manual
        open / close.

        1)
        with FWFFile(HumanFile).open(DATA) as fd:
            assert fd.mm is not None

        2)
        fd = fwf.open(DATA)
        ...
        fd.close()
        """

        return self


    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


    def calculate_line_count(self, _mm) -> int:
        """Calculate the number of lines in the file"""
        return int((self.fsize - self.start_pos + 0.1) / self.fwidth) if len(_mm) > 0 else 0


    def open(self, file) -> 'FWFFile':
        """Initialize the fwf table with a file"""

        if file is None:
            raise FWFFileException("'file' must not be None")

        if isinstance(file, str):
            self.file = file
            _fd = self._fd = open(file, "rb")
            _mm = mmap.mmap(_fd.fileno(), 0, access=mmap.ACCESS_READ)
        elif isinstance(file, bytes):
            # Support data already loaded in whatever way. Nice for testing.
            self.file = id(file)
            _mm = file
        else:
            raise FWFFileException(f"Invalid 'file' argument. Must be of type str or bytes: {type(file)}")

        self._mm = memoryview(_mm)
        self.initialize()

        return self


    def initialize(self) -> None:
        """Determine the newline byte(s), start_pos, line length, etc."""

        assert self._mm is not None
        self.number_of_newline_bytes = self._number_of_newline_bytes(self._mm)
        self.fsize = self.get_file_size(self._mm)
        self.start_pos = self.skip_comment_line(self._mm, self.comments)
        # The length of each line
        self.fwidth = self.record_length(self._mm, self.fields, self.start_pos) + self.number_of_newline_bytes
        self.line_count = self.calculate_line_count(self._mm)


    def close(self) -> None:
        """Close the file and all open handles"""

        if self._fd:
            if self._mm:
                try:
                    self._mm.close()  # type: ignore
                except AttributeError:
                    pass

            self._fd.close()

        self._mm = self._fd = None


    def count(self) -> int:
        """Return the number of records in the file"""
        return self.line_count


    def get_parent(self) -> None:
        return None


    def _parent_index(self, index: int) -> int:
        return index


    def pos_from_index(self, index: int) -> int:
        """Determine the position of the first byte within the file
        for the line with the index provided"""

        if index < 0:
            index = self.line_count + index

        assert index >= 0, f"Index must be >= 0: {index}"

        pos = self.start_pos + (index * self.fwidth)
        if pos <= self.fsize:
            return pos

        raise IndexError(f"Invalid index: {index}")


    def _raw_line_at(self, index: int) -> memoryview:
        """Get the raw line data for the line with the index"""
        assert self._mm is not None
        pos = self.pos_from_index(index)
        return self._mm[pos : pos + self.fwidth]


    def _fwf_by_indices(self, indices: list[int]) -> FWFSubset:
        return FWFSubset(self, indices)


    def _fwf_by_slice(self, start: int, stop: int) -> FWFRegion:
        return FWFRegion(self, start, stop)


    def iter_lines(self) -> Iterator[memoryview]:
        """Iterate over all the lines in the file"""

        assert self._mm is not None

        start_pos = self.start_pos or 0
        end_pos = self.fsize or 0
        fwidth = self.fwidth or 0
        irow = 0
        end = start_pos + fwidth
        while end <= end_pos:
            # rtn = memoryview(self._mm[start_pos : end])  # It is not getting faster
            # rtn = self._mm[start_pos : end]  # TODO This is where python copies the memory
            yield self._mm[start_pos : end]
            start_pos = end
            end = start_pos + fwidth
            irow += 1


    def iter_lines_with_field(self, field) -> Iterator[memoryview]:
        """An optimized version that iterates over a single field in all lines.
        This is useful for unique and index.
        """
        assert self._mm is not None
        assert self.fwidth is not None

        fslice = self.fields[field]
        flen = fslice.stop - fslice.start
        start_pos = self.start_pos + fslice.start
        end_pos = self.fsize or 0
        fwidth = self.fwidth
        for start_pos in range(start_pos, end_pos, fwidth):
            # rtn = self._mm[start_pos : start_pos + flen]  # This is where python copies the memory
            yield self._mm[start_pos : start_pos + flen]
