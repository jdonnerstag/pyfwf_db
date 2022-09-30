#!/usr/bin/env python
# encoding: utf-8

"""A base class that defines a view-like object"""

import abc
import sys
from typing import overload, Callable, Iterator, Iterable, Optional
from collections import OrderedDict
from itertools import islice
from prettytable import PrettyTable

from .fwf_fieldspecs import FWFFileFieldSpecs
from .fwf_line import FWFLine


class FWFViewLike:
    """A core class. Provide all the necessary basics to implement different
    kind of views, such as views based on a slice, or views based on
    indiviual indexes.
    """

    def __init__(self, filespec, parent: Optional['FWFViewLike']):
        self.parent = parent
        if parent is not None:
            self.filespec = parent.filespec
            self.fields = parent.fields
            fields = tuple(parent.field_getter.keys())
        else:
            self.filespec = filespec() if isinstance(filespec, type) else filespec
            self.fields = FWFFileFieldSpecs(getattr(self.filespec, "FIELDSPECS"))
            fields = self._default_header()

        # Map all field names to functions able to determine the field value
        self.field_getter = self._determine_all_field_getters(*fields)


    def __len__(self) -> int:
        """Varies depending on the view implementation"""
        return self.count()


    @abc.abstractmethod
    def count(self) -> int:
        """Provide the number of records in this view"""


    def validate_index(self, index: int) -> int:
        """Validate and normalize the index"""

        xlen = len(self)
        if index < 0:
            index = xlen + index

        if 0 <= index < xlen:
            return index

        raise IndexError(f"Invalid index: 0 >= index < {xlen}: {index}")


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

        if self.parent is None:
            return self, index

        index = self._parent_index(index)
        return self.parent.root(index, stop_view)


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
        if isinstance(idx, int):
            return next(islice(self.fields.keys(), idx, None))

        return idx


    def field_dtype(self, field) -> str:
        """Return the dtype for the field. NOTE: currently only string types are returned"""
        field = self.fields[self.field_from_index(1)]
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
        sslice: slice = self.fields[field].slice
        gen = (line[sslice] for line in self.iter_lines())
        return gen


    def filter(self, *args: Callable, is_or: bool=False) -> 'FWFViewLike':
        """Apply filters (keep) and return a new view."""
        func = any if is_or else all
        return self.filter_by_line(lambda x: func(arg(x) for arg in args))


    def exclude(self, *args: Callable, is_or: bool=False) -> 'FWFViewLike':
        """Apply filters (remove) and return a new view."""
        func = any if is_or else all
        return self.filter_by_line(lambda x: not func(arg(x) for arg in args))


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


    def order_by(self, *names: str) -> 'FWFViewLike':
        """Create a new view with the line ordered based on the
        field names provided.

        Default ordering is ascending. Decending ordering can be achieved
        by prepending a '-', e.g. '-birthday'
        """

        if not names:
            return self

        # TODO This list can grow large. Leverage numpy??
        indexes = list(range(self.count()))

        indexes.sort(key=lambda i: _FWFSort(self, i, names))

        return self.fwf_by_indices(indexes)


    def unique(self, *fields: str) -> list[bytes|tuple[bytes]]:
        """Determine the unique values for one or more fields"""

        assert fields, "You must provide at least one field name"

        idx_dict: set[bytes|tuple[bytes]] = set()
        for line in self:
            if len(fields) == 1:
                data = line[fields[0]]
            else:
                data = line.to_list(*fields)
            idx_dict.add(data)

        return list(idx_dict)


    def _default_header(self) -> tuple[str]:
        """Determine the default list of header fields for this view"""
        func = getattr(self.filespec, "__header__", None)
        if callable(func):
            header = func()
        else:
            header = self.fields.names()

        return tuple(header)


    def get_rooted_lineno(self, line: FWFLine) -> int:
        """Get line number of this line in the file (vs the view)."""
        _, lineno = self.root(line.lineno)
        return lineno


    def get_field_line(self, line: FWFLine) -> bytes:
        """Return the bytes representing the line"""
        return line.get_line()


    def get_file_name(self, line: FWFLine) -> str:
        """Get the file name"""
        root, _ = self.root(line.lineno)
        file = getattr(root, "file")
        return "<literal>" if isinstance(file, int) else file


    def get_with_fieldspec(self, field: str) -> Callable:
        """Return a function that retrieves the data for field from a line"""
        field_slice: slice = self.fields[field]["slice"]
        return lambda line: bytes(line.line[field_slice])


    def get_with_filespec_func(self, field: str) -> Callable:
        """If it exists, return from filespec the method which names
        is equal to the field"""
        return getattr(self.filespec, field)


    def getter_for_field(self, field: str) -> Callable:
        """Determine the function that will be used to retrieve the
        field's value from a line"""

        if field == "_lineno":
            rtn = self.get_rooted_lineno
        elif field == "_line":
            rtn = self.get_field_line
        elif field == "_file":
            rtn = self.get_file_name
        else:
            try:
                rtn = self.get_with_fieldspec(field)
            except KeyError:
                try:
                    rtn = self.get_with_filespec_func(field)
                except AttributeError:
                    # pylint: disable=raise-missing-from
                    raise KeyError(f"Filespec has no field with name: '{field}'")

        return rtn


    def _determine_all_field_getters(self, *fields: str) -> OrderedDict[str, Callable]:
        if not fields:
            try:
                fields = tuple(self.field_getter.keys())
            except AttributeError:
                fields = self._default_header()

        return OrderedDict((field, self.getter_for_field(field)) for field in fields)


    def header(self) -> tuple[str]:
        """Return the current list of columns in the header"""
        return tuple(self.field_getter.keys())


    def set_header(self, *fields: str) -> 'FWFViewLike':
        """Change the order and or selection of columns"""
        self.field_getter = self._determine_all_field_getters(*fields)
        return self


    def add_field(self, name:str, **kwargs) -> None:
        """Add an additional field to the header"""
        self.fields.add_field(name, **kwargs)
        self.field_getter[name] = self.getter_for_field(name)


    def update_field(self, name:str, **kwargs) -> None:
        """Update an existing field"""
        self.fields.update_field(name, **kwargs)


    def to_list(self, *fields: str, stop: int = -1, header: bool = False) -> Iterator[tuple]:
        """Create an ordinary python list from the view"""

        getter = self._determine_all_field_getters(*fields)
        fields = tuple(getter.keys())

        if header:
            yield fields

        stop = self.count() if stop < 0 else min(self.count(), stop)
        for i in range(stop):
            values = tuple(func(self[i]) for _, func in getter.items())
            yield values


    def validate(self, func: None|Callable = None) -> 'FWFViewLike':
        """Basically a filter, it either uses the filespec __validate__()
        method to filter (validate) records, or the function provided"""

        if func is None:
            func = getattr(self.filespec, "__validate__")
            assert callable(func)

        return self.filter(func)


    def parse(self, func: None|Callable = None, stop: int = -1):
        """Iterate over all records in the view and either invoke
        the filespec __parse__() method, or the method provided """

        if func is None:
            func = getattr(self.filespec, "__parse__")
            assert callable(func)

        stop = self.count() if stop < 0 else min(self.count(), stop)
        for i in range(stop):
            yield func(self[i])


    def get_pretty_string(self, *fields: str, stop: int = 10) -> str:
        """Create a string representation of the data"""
        stop = self.count() if stop < 0 else min(self.count(), stop)
        gen = self.to_list(*fields, stop=stop, header=True)
        fields = next(gen)
        rtn = PrettyTable()
        rtn.field_names = fields
        gen = (tuple(str(v, "utf-8") if isinstance(v, bytes) else v for v in row) for row in gen)
        rtn.add_rows(gen)
        return rtn.get_string() + f"\n  len: {stop:,}/{self.count():,}"


    def get_string(self, *fields: str, stop: int = 10, pretty: bool = True) -> str:
        """Create a string representation of the data"""
        stop = self.count() if stop < 0 else min(self.count(), stop)

        if pretty:
            return self.get_pretty_string(*fields, stop=stop)

        rtn = f"{self.__class__.__name__}(count={self.count()}):\n"
        data = self.to_list(*fields, stop=stop, header=True)
        rtn += "[" + ",\n  ".join(str(f) for f in data)
        if 0 <= stop < self.count():
            rtn += "\n  ..."

        rtn += "\n]\n"
        return rtn


    def print(self, *fields: str, stop: int=10, pretty: bool=True, file=sys.stdout) -> None:
        """Print the table content"""
        print(self.get_string(*fields, stop=stop, pretty=pretty), file=file)


    def __str__(self) -> str:
        return self.get_string(stop=10, pretty=True)


    def __repr__(self) -> str:
        return self.get_string(stop=10, pretty=False)

# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------

class _FWFSort:
    """Support order_by and unique for FWFViewLike objects"""

    def __init__(self, fwf_view: FWFViewLike, i: int, names: tuple[str]) -> None:
        self.fwf_view = fwf_view
        self.line = fwf_view.line_at(i)
        self.names = [(name[1:], True) if name.startswith("-") else (name, False) for name in names]


    def __lt__(self, other: '_FWFSort') -> bool:
        for name, reverse in self.names:
            # TODO we might be able to improve performance with a cmp() type of approach, and memoryview
            bytes_1 = bytes(self.line[name])
            bytes_2 = bytes(other.line[name])
            if reverse is False:
                if bytes_1 < bytes_2:
                    return True
                if bytes_1 > bytes_2:
                    return False
            else:
                if bytes_1 < bytes_2:
                    return False
                if bytes_1 > bytes_2:
                    return True

        return False
