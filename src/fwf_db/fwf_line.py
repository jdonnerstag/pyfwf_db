#!/usr/bin/env python
# encoding: utf-8

import typing
from typing import Iterator, Tuple, Union, overload

import sys
from datetime import datetime

# To prevent circular dependencies only during type checking
if typing.TYPE_CHECKING:
    from .fwf_view_like import FWFViewLike


class FWFLine:
    """A dictionary like convinience class to access the fields within a
    line. Access is similar to dict() with get(), [], keys, in, ...
    """

    # Note: 'int' and 'str' is required because of str() and int()
    def __init__(self, fwf_file_like: 'FWFViewLike', lineno: 'int', line: bytes):
        assert fwf_file_like is not None
        #assert isinstance(lineno, int)     Numpy provides a int-like object

        self.fwf_file_like = fwf_file_like
        self.lineno = lineno
        self.line = line

    @overload
    def __getitem__(self, arg: 'int') -> 'int': ...

    @overload
    def __getitem__(self, arg: 'str') -> bytes: ...

    @overload
    def __getitem__(self, arg: slice) -> bytes: ...

    def __getitem__(self, arg: Union['str', 'int', slice]) -> Union['int', bytes]:
        """Get a field or range of bytes from the line.

        string: representing a field name, get the data associated with it.
        int: get the byte at the position
        slice: get a bytearray for that slice
        """
        if isinstance(arg, str):
            return self._get(arg)
        if isinstance(arg, int):
            return self.line[arg]
        if isinstance(arg, slice):
            return self.line[arg]

        raise IndexError(f"Invalid Index: {arg}")


    def _get(self, field: 'str') -> bytes:
        """Get the binary data for the field"""
        field_slice: slice = self.fwf_file_like.fields[field]
        return self.line[field_slice]


    def get(self, field, default:bytes|None=None) -> bytes|None:
        """Get the binary data for the field"""
        rtn = self._get(field)
        if rtn:
            return rtn

        return default


    def str(self, field, encoding=None) -> str:
        """Get the data for the field converted into a string, optionally
        applying an encoding
        """
        encoding = encoding or sys.getdefaultencoding()
        return str(self._get(field), encoding)


    def int(self, field) -> int:
        """Get the data for the field converted into an int"""
        return int(self._get(field))


    def date(self, field, fmt="%Y%m%d") -> datetime:
        """Get the data for the field converted into an datetime object
        applying the 'format'
        """
        rtn = self.str(field, None)
        rtn = datetime.strptime(rtn, fmt)
        return rtn


    def __contains__(self, key) -> bool:
        """suppot pythons 'in' operator"""
        return key in self.fwf_file_like.fields


    def keys(self) -> Iterator['str']:
        """Like dict's keys() method, return all field names"""
        return self.fwf_file_like.fields.keys()


    def items(self) -> Iterator[Tuple['str', bytes]]:
        """Like dict's items(), return field name and value tuples"""
        for key in self.keys():
            rtn = self._get(key)
            yield (key, rtn)


    def to_dict(self) -> dict:
        """Provide the line as dict"""
        return dict(self.items())


    def to_list(self, keys=None) -> list:
        """Provide a values in a a list"""
        keys = keys or self.keys()
        return [self._get(key) for key in keys]


    def __repr__(self) -> 'str':
        return str(self.line)
