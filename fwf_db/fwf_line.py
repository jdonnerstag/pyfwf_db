#!/usr/bin/env python
# encoding: utf-8

"""
"""

from datetime import datetime


class FWFLine(object):
    """   """

    def __init__(self, parent, idx, line, columns):

        assert isinstance(idx, int)
        assert isinstance(columns, dict)

        self.parent = parent
        self.idx = idx
        self.line = line
        self.columns = columns


    def __len__(self):
        return len(self.columns)


    def get_raw(self, field):
        return self.line[self.columns(field)]


    def get(self, field):
        return self.get_raw(field).to_bytes()


    def str(self, field, encoding):
        return self.get_raw(field).to_bytes().decode(encoding)


    def int(self, field):
        return int(self.get_raw(field).to_bytes())


    def date(self, field, format="%Y%m%d"):
        rtn = self.str(field, None)
        rtn = datetime.strptime(rtn, format)
        return rtn
