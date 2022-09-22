#!/usr/bin/env python
# encoding: utf-8

"""A base class that defines a view-like object"""

import abc
import sys
from typing import overload, Callable, Iterator, Iterable, Optional
from collections import OrderedDict
from itertools import islice
from prettytable import PrettyTable

from .fwf_fieldspecs import FWFFileFieldSpecs, FWFFieldSpec
from .fwf_line import FWFLine


class FWFViewLike:
    """A core class. Provide all the necessary basics to implement different
    kind of views, such as views based on a slice, or views based on
    indiviual indexes.
    """

    def __init__(self, fields: FWFFileFieldSpecs):
        self.fields = fields


    def __len__(self) -> int:
        """Varies depending on the view implementation"""
        return self.count()


    @abc.abstractmethod
    def count(self) -> int:
        """Provide the number of records in this view"""


    def get_fields(self) -> FWFFileFieldSpecs:
        """Return the fieldspec for all the fields"""
        return self.fields


    def field(self, index: str) -> FWFFieldSpec:
        """Return the fieldspec for the field"""
        return self.fields[index]


    def validate_index(self, index: int) -> int:
        """Validate and normalize the index"""

        xlen = len(self)
        if index < 0:
            index = xlen + index

        if 0 <= index < xlen:
            return index

        raise IndexError(f"Invalid index: 0 >= index < {xlen}: {index}")


    @abc.abstractmethod
    def get_parent(self) -> Optional['FWFViewLike']:
        """Return the parent"""


    @abc.abstractmethod
    def _parent_index(self, index: int) -> int:
        """Determine the index in the context of the parent view"""


    def parent_index(self, index: int) -> int:
        """Determine the index in the context of the parent view"""
        index = self.validate_index(index)
        return self._parent_index(index)


    def root(self, index: int, stop_view: Optional['FWFViewLike'] = None) -> tuple['FWFViewLike', int]:
        """Walk up the parent path and determine the most outer
        view-like object and the line number.

        Note that this function is NOT validating the index value. It
        simply applies the mapping from one view to its parent.
        """
        if (stop_view is not None) and (self == stop_view):
            return self, index

        parent = self.get_parent()
        if parent is None:
            return self, index

        index = self._parent_index(index)
        return parent.root(index, stop_view)


    @abc.abstractmethod
    def _raw_line_at(self, index: int) -> memoryview:
        """Get the raw line data (bytes) for the line with the index"""


    def raw_line_at(self, index: int) -> memoryview:
        """Get the raw line data (bytes) for the line with the index"""
        index = self.validate_index(index)
        return self._raw_line_at(index)


    def line_at(self, index: int) -> FWFLine:
        """Get the line data for the line with the index"""
        data = self.raw_line_at(index)
        return FWFLine(self, index, data)


    @abc.abstractmethod
    def _fwf_by_indices(self, indices: list[int]) -> 'FWFViewLike':
        """Initiate a FWFSubset (or similar) object and return it"""


    def fwf_by_indices(self, indices: list[int]) -> 'FWFViewLike':
        """Initiate a FWFSubset (or similar) object and return it"""
        indices = [self.validate_index(i) for i in indices]
        return self._fwf_by_indices(indices)


    @abc.abstractmethod
    def _fwf_by_slice(self, start: int, stop: int) -> 'FWFViewLike':
        """Initiate a FWFRegion (or similar) object and return it"""


    def fwf_by_slice(self, region: slice) -> 'FWFViewLike':
        """Initiate a FWFRegion (or similar) object and return it"""
        start = self._normalize_index(region.start, 0)
        stop = self._normalize_index(region.stop, len(self))
        assert start <= stop, f"Invalid slice: start <= stop; start={start}, stop={stop}"

        return self._fwf_by_slice(start, stop)


    def field_from_index(self, idx):
        """Determine the field name from the index"""
        fields = self.get_fields()
        if isinstance(idx, int):
            return next(islice(fields.keys(), idx, None))

        return idx


    def field_dtype(self, field) -> str:
        """Return the dtype for the field. NOTE: currently on string types are returned"""
        field = self.get_fields()[self.field_from_index(1)]
        flen = field.stop - field.start
        return f"S{flen}"


    def _normalize_index(self, index: int, default: int) -> int:
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
            row_idx = self.validate_index(row_idx)
            return self.line_at(row_idx)

        if isinstance(row_idx, slice):
            return self.fwf_by_slice(row_idx)

        if all(isinstance(x, bool) for x in row_idx):
            # TODO this is rather slow for large indexes
            idx = [i for i, v in enumerate(row_idx) if v is True]
            return self.fwf_by_indices(idx)

        if all(isinstance(x, int) for x in row_idx):
            # Don't allow the subset to grow
            row_idx = [i for i in row_idx]
            return self.fwf_by_indices(row_idx)

        raise KeyError(f"Invalid range value: {row_idx}")


    def __iter__(self) -> Iterator[FWFLine]:
        for lineno, line in enumerate(self.iter_lines()):
            yield FWFLine(self, lineno, line)


    @abc.abstractmethod
    def iter_lines(self) -> Iterator[memoryview]:
        """Iterate over all lines in the view, returning the raw line data"""


    def iter_lines_with_field(self, field) -> Iterator[memoryview]:
        """Iterate over all lines in the file returning the raw field data"""
        sslice: slice = self.get_fields()[field].fslice
        gen = (line[sslice] for line in self.iter_lines())
        return gen


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


    def filter_by_line(self, func: Callable[[FWFLine], bool]) -> 'FWFViewLike':
        """Filter lines with a condition

        Iterate over all lines in the file and apply 'func' to every line. Except
        if 'func' returns True, the line will be skipped.

        The result is a view on the data, rather then copies.
        """

        rtn = [i for i, rec in enumerate(self) if func(rec)]
        return self.fwf_by_indices(rtn)


    def filter_by_field(self, field: str, func) -> 'FWFViewLike':
        """Filter lines by the 'field' and 'func' provided.

        Iterate over all lines in the file, determine the (byte) value for the field,
        and apply 'func' to that field value. Except if 'func' returns True, the
        line will be skipped.

        The result is a view on the data, rather then copies.
        """

        assert isinstance(field, str), f"'field' must be a string: {field}"

        gen = enumerate(self.iter_lines_with_field(field))
        if callable(func):
            rtn = [i for i, rec in gen if func(rec)]
        else:
            rtn = [i for i, rec in gen if rec == func]

        return self.fwf_by_indices(rtn)


    def headers(self, *fields: str) -> tuple[str]:
        """Get the names for all fields"""
        if not fields:
            return tuple(self.fields.keys())

        rtn = set()
        for field in fields:
            if field in ["_lineno", "_line"]:
                rtn.add(field)
            elif field in self.fields:
                rtn.add(field)

        return tuple(rtn)


    def to_list(self, *fields: str, stop: int = -1, header: bool = True) -> Iterator[tuple]:
        """Create an ordinary python list from the view"""

        if header:
            yield self.headers()

        if stop <= 0:
            stop = self.count()

        stop = min(self.count(), stop)
        for i in range(stop):
            values = self[i].to_list(*fields)
            yield values


    def get_string(self, *fields: str, stop: int = -1, pretty: bool = True) -> str:
        """Create a string representation of the data"""
        if pretty:
            rtn = PrettyTable()
            rtn.field_names = self.headers(*fields)
            rtn.add_rows(self.to_list(*fields, stop=stop, header=False))
            return rtn.get_string() + f"\n  len: {self.count():,}"

        rtn = f"{self.__class__.__name__}(count={self.count()}):\n"
        data = self.to_list(*fields, stop=stop, header=True)
        rtn += "[" + ",\n  ".join(str(f) for f in data)
        if 0 <= stop < self.count():
            rtn += "..."

        rtn += "\n]\n"
        return rtn


    def print(self, *fields: str, stop: int=-1, pretty: bool=True, file=sys.stdout) -> None:
        """Print the table content"""
        print(self.get_string(*fields, stop=stop, pretty=pretty), file=file)


    def __str__(self) -> str:
        return self.get_string(stop=10, pretty=False)


    def __repr__(self) -> str:
        return self.get_string(stop=10, pretty=True)
