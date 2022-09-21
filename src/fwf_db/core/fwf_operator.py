#!/usr/bin/env python
# encoding: utf-8

import sys
from typing import Callable, Any

from .fwf_line import FWFLine

class FWFOperator:
    """ Easily define filter criteria

    Examples:
    rtn = fwf.filter(op("gender") < b"M")
    rtn = fwf.filter(op("gender").str() == "F")
    rtn = fwf.filter(op("gender").str().strip() == "F")
    """

    def __init__(self, name: 'str', func: None|Callable[[memoryview], Any]=None):
        self.name = name
        self.func: Callable[[memoryview], Any] = func if func is not None else lambda x: x

    def get(self, line: FWFLine) -> Any:
        """ Apply the function to the field's data from within the line """
        return self.func(line[self.name])

    def __eq__(self, other):
        return lambda line: self.get(line) == other

    def __ne__(self, other):
        return lambda line: self.get(line) != other

    def __gt__(self, other):
        return lambda line: self.get(line) > other

    def __lt__(self, other):
        return lambda line: self.get(line) < other

    def __ge__(self, other):
        return lambda line: self.get(line) >= other

    def __le__(self, other):
        return lambda line: self.get(line) <= other

    def is_in(self, other):
        """ Apply the 'in' operator to the field's value """
        return lambda line: self.get(line) in other

    def is_notin(self, other):
        """ Apply the 'not in' operator to the field's value """
        return lambda line: self.get(line) not in other

    def bytes(self):
        orig = self.func
        self.func = lambda x: bytes(orig(x))
        return self

    def str(self, encoding=None):
        """ Convert the field's byte value into a string """
        encoding = encoding or sys.getdefaultencoding()
        orig = self.func
        self.func = lambda x: str(orig(x), encoding)
        return self

    def strip(self):
        """ Strip the field's value """
        orig = self.func
        self.func = lambda x: orig(x).strip()
        return self

    def lower(self):
        """ Convert the field's value to lowercase """
        orig = self.func
        self.func = lambda x: orig(x).lower()
        return self

    def upper(self):
        """ Convert the field's value to uppercase """
        orig = self.func
        self.func = lambda x: orig(x).upper()
        return self

    def int(self):
        """ Convert the field's value into an integer """
        orig = self.func
        self.func = lambda x: int(orig(x))
        return self
