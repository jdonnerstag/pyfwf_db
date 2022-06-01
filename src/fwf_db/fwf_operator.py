#!/usr/bin/env python
# encoding: utf-8

import sys


class FWFOperator:

    def __init__(self, name, func=None):
        self.name = name
        self.func = func

        if func is None:
            self.func = lambda x: x

    def get(self, line):
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
        return lambda line: self.get(line) in other

    def is_notin(self, other):
        return lambda line: self.get(line) not in other

    def str(self, encoding=None):
        self.encoding = encoding or sys.getdefaultencoding()
        orig = self.func
        self.func = lambda x: str(orig(x), self.encoding)
        return self

    def strip(self):
        orig = self.func
        self.func = lambda x: orig(x).strip()
        return self

    def lower(self):
        orig = self.func
        self.func = lambda x: orig(x).lower()
        return self

    def upper(self):
        orig = self.func
        self.func = lambda x: orig(x).upper()
        return self

    def int(self):
        orig = self.func
        self.func = lambda x: int(orig(x))
        return self
