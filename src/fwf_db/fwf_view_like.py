#!/usr/bin/env python
# encoding: utf-8

import abc
from typing import Iterator, Tuple, Callable, overload, Iterable
from itertools import islice

from .fwf_base_mixin import FWFBaseMixin
from .fwf_line import FWFLine

class FWFViewLike(FWFBaseMixin, metaclass=abc.ABCMeta):
    """A core class. Provide all the necessary basics to implement different
    kind of views, such as views based on a slice, or views based on
    indiviual indexes.
    """

    # TODO Why not __init__() ??
    def init_view_like(self, lines, fields):
        assert lines is not None
        assert fields is not None

        self.lines = lines
        self.fields = fields


    @abc.abstractmethod
    def __len__(self) -> int:
        """Varies depending on the view implementation"""


    @abc.abstractmethod
    def line_at(self, index) -> bytes:
        """Get the raw line data for the line with the index"""


    @abc.abstractmethod
    def fwf_by_indices(self, indices) -> 'FWFViewLike':
        """Initiate a FWFLine (or similar) object and return it"""


    @abc.abstractmethod
    def fwf_by_slice(self, arg) -> 'FWFViewLike':
        """Initiate a FWFRegion (or similar) object and return it"""


    @abc.abstractmethod
    def fwf_by_line(self, idx, line) -> FWFLine:
        """Initiate a FWFRegion (or similar) object and return it"""


    def field_from_index(self, idx):
        """Determine the field name from the index"""
        fields = self.fields
        if isinstance(idx, int):
            return next(islice(fields.keys(), idx, None))

        return idx


    def field_dtype(self, field) -> str:
        """Return the dtype for the field. NOTE: currently on string types are returned"""
        field = self.fields[self.field_from_index(1)]
        flen = field.stop - field.start
        return f"S{flen}"


    def normalize_index(self, index: int, default: int) -> int:
        """For start and stop values of a slice, determine sensible
        default when the index is None or < 0
        """
        if index is None:
            index = default
        elif index < 0:
            index = len(self) + index

        assert index >= 0, f"Invalid index: must be >= 0: {index}"
        assert index <= len(self), f"Invalid index: must <= len: {index}"

        return index


    @overload
    def __getitem__(self, row_idx: int) -> FWFLine: ...

    @overload
    def __getitem__(self, row_idx: slice) -> 'FWFViewLike': ...

    @overload
    def __getitem__(self, row_idx: Iterable[bool]) -> 'FWFViewLike': ...

    @overload
    def __getitem__(self, row_idx: Iterable[int]) -> 'FWFViewLike': ...

    def __getitem__(self, row_idx):
        """Provide support for [..] access: slice by row and column

        Examples:
            fwf[0]
            fwf[0:5]
            fwf[-1]
            fwf[-5:]
            fwf[:]
            fwf[1, 5, 10, -1]
            fwf[True, True, False, True]
        """

        if isinstance(row_idx, int):
            row_idx = self.normalize_index(row_idx, 0)
            return self.fwf_by_line(row_idx, self.line_at(row_idx))

        if isinstance(row_idx, slice):
            start = self.normalize_index(row_idx.start, 0)
            stop = self.normalize_index(row_idx.stop, len(self))
            return self.fwf_by_slice(slice(start, stop))

        if all(isinstance(x, bool) for x in row_idx):
            # TODO this is rather slow for large indexes
            idx = [i for i, v in enumerate(row_idx) if v is True]
            return self.fwf_by_indices(idx)

        if all(isinstance(x, int) for x in row_idx):
            # Don't allow the subset to grow
            row_idx = [self.normalize_index(x, -1) for x in list(row_idx)]
            return self.fwf_by_indices(row_idx)

        raise IndexError(f"Invalid range value: {row_idx}")


    def __iter__(self) -> Iterator[FWFLine]:
        """iterate over all rows.

        Return an object describing the line and providing access to
        each field.
        """

        return self.iter()


    def iter(self) -> Iterator[FWFLine]:
        """iterate over all rows.

        Return an object describing the line and providing access to
        each field.
        """

        for i, line in self.iter_lines():
            rtn = self.fwf_by_line(i, line)
            yield rtn


    @abc.abstractmethod
    def iter_lines(self) -> Iterator[Tuple[int, bytes]]:
        """Iterate over all lines in the file, returning raw line data (bytes)"""


    def filter(self, arg1: str|Callable, arg2=None) -> 'FWFViewLike':
        """Filter either by line or by field.

        If the first parameter is callable, then filter by line.
        Else the first parameter must be a valid field name, and the second
        parameter a callable or any fixed value.
        """
        if isinstance(arg1, str):
            return self.filter_by_field(arg1, arg2)

        if isinstance(arg1, Callable):
            return self.filter_by_line(arg1)

        raise AttributeError(f"filter(): Invalid arguments: arg1={arg1}, arg2={arg2}")


    def filter_by_line(self, func: Callable) -> 'FWFViewLike':
        """Filter lines with a condition

        Iterate over all lines in the file and apply 'func' to every line. Except
        if 'func' returns True, the line will be skipped.

        The result is a view on the data, rather then copies.
        """

        rtn = [i for i, rec in self.iter_lines() if func(self.fwf_by_line(i, rec))]
        return self.fwf_by_indices(rtn)


    def filter_by_field(self, field: str, func) -> 'FWFViewLike':
        """Filter lines by the 'field' and 'func' provided.

        Iterate over all lines in the file, determine the (byte) value for the field,
        and apply 'func' to that field value. Except if 'func' returns True, the
        line will be skipped.

        The result is a view on the data, rather then copies.
        """

        assert isinstance(field, str), f"'field' must be a string: {field}"
        sslice = self.fields[field]

        if callable(func):
            rtn = [i for i, rec in self.iter_lines() if func(rec[sslice])]
        else:
            rtn = [i for i, rec in self.iter_lines() if rec[sslice] == func]

        return self.fwf_by_indices(rtn)
