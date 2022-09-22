#!/usr/bin/env python
# encoding: utf-8

from collections import OrderedDict
from typing import Iterable, Union, Optional, overload, TYPE_CHECKING, Any

import sys
from datetime import datetime

from .fwf_fieldspecs import FWFFieldSpec


# To prevent circular dependencies only during type checking
if TYPE_CHECKING:
    from .fwf_view_like import FWFViewLike


class FWFLine:
    """A dictionary like convinience class to access the fields within a
    line. Access is similar to dict() with get(), [], keys, in, ...
    """

    # Note: 'int' and 'str' is required because of str() and int()
    def __init__(self, fwf_view: 'FWFViewLike', lineno: 'int', line: memoryview):
        assert fwf_view is not None
        #assert isinstance(lineno, int)     Numpy provides a int-like object

        self.fwf_view: 'FWFViewLike' = fwf_view
        self.lineno: int = lineno    # Line number in the context of 'fwf_view'
        self.line: memoryview = line

    @overload
    def __getitem__(self, arg: 'int') -> 'int': ...

    @overload
    def __getitem__(self, arg: 'str') -> memoryview: ...

    @overload
    def __getitem__(self, arg: FWFFieldSpec) -> memoryview: ...

    @overload
    def __getitem__(self, arg: slice) -> memoryview: ...

    def __getitem__(self, arg: Union['str', 'int', slice, FWFFieldSpec]) -> memoryview|int:
        """Get a field or range of bytes from the line.

        fieldspec: return the line data associated with the fieldspec (slice)
        string: identify the field by its name and return the data associated with it
        int: return the byte at the position provided
        slice: return the bytes associated with the slice
        """
        rtn = self.get(arg)
        if rtn is not None:
            return rtn

        raise KeyError(f"Invalid Index: {arg}")


    def _get(self, field: 'str') -> memoryview:
        """Get the binary data for the field"""
        field_slice: slice = self.fwf_view.fields[field].fslice
        return self.line[field_slice]


    def get(self, arg: Union['str', 'int', slice, FWFFieldSpec], default = None):
        """Get a field or range of bytes from the line.

        fieldspec: return the line data associated with the fieldspec (slice)
        string: identify the field by its name and return the data associated with it
        int: return the byte at the position provided
        slice: return the bytes associated with the slice
        """
        if arg == "_lineno":
            return self.lineno
        if arg == "_line":
            return self.line
        if isinstance(arg, FWFFieldSpec):
            return self.line[arg.fslice]
        if isinstance(arg, str):
            return self._get(arg)
        if isinstance(arg, int):
            return self.line[arg]
        if isinstance(arg, slice):
            return self.line[arg]
        return default


    def str(self, field: str, encoding=None) -> str:
        """Get the data for the field converted into a string, optionally
        applying an encoding
        """
        encoding = encoding or sys.getdefaultencoding()
        return str(self[field], encoding)


    def int(self, field) -> int:
        """Get the data for the field converted into an int"""
        return int(self[field])


    def date(self, field, fmt="%Y%m%d") -> datetime:
        """Get the data for the field converted into an datetime object
        applying the 'format'
        """
        rtn = self.str(field, None)
        rtn = datetime.strptime(rtn, fmt)
        return rtn


    def __contains__(self, key) -> bool:
        """Suppot pythons 'in' operator"""
        return key in self.fwf_view.fields


    def keys(self):
        """Like dict's keys() method, return all field names"""
        return self.fwf_view.fields.keys()


    def items(self, *keys: 'str', to_bytes: bool = True) -> Iterable[tuple['str', Any]]:
        """Similar to dict's items(), return field name and value tuples"""

        names = self.fwf_view.headers(*keys)
        for key in names:
            data = self[key]
            if to_bytes:
                data = bytes(data)

            yield (key, data)


    def to_dict(self, *keys: 'str') -> OrderedDict['str', Any]:
        """Provide the line as dict"""
        return OrderedDict(self.items(*keys, to_bytes=True))


    def to_list(self, *keys: 'str') -> tuple[Any]:
        """Provide all values in a list"""
        return tuple(v for _, v in self.items(*keys, to_bytes=True))


    def rooted(self, stop_view: Optional['FWFViewLike'] = None) -> 'FWFLine':
        """Walk up the parent path and determine the most outer
        view-like object and the line number.

        Note that this function is NOT validating the index value. It
        simply applies the mapping from one view to its parent.
        """
        view, lineno = self.fwf_view.root(self.lineno, stop_view)
        return FWFLine(view, lineno, self.line)


    def __repr__(self) -> 'str':
        return str(self.line)
