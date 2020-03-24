#!/usr/bin/env python
# encoding: utf-8

"""
"""

from datetime import datetime


class FWFLine(object):
    """   """

    def __init__(self, parent, idx, line, columns=None):

        assert parent is not None

        assert isinstance(idx, int)
        assert columns is None or isinstance(columns, list)

        self.parent = parent
        self.idx = idx
        self.columns = parent.columns   # It a bit irritating. This is a dict

        # whereas the argument columns is a list
        if columns:
            self.columns = {k: v for k, v in self.columns.items() if k in columns}

        self.idx = idx
        self.line = line 


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
