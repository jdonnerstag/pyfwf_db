#!/usr/bin/env python
# encoding: utf-8

from datetime import datetime


class FWFLine(object):
    def __init__(self, fwf_file_like, lineno, line):
        assert fwf_file_like is not None
        assert isinstance(lineno, int)

        self.fwf_file_like = fwf_file_like     
        self.lineno = lineno
        self.line = line

    def __getitem__(self, arg):
        if isinstance(arg, str):
            return self.get(arg)
        elif isinstance(arg, (int, slice)):
            return self.line[arg]
        else:
            raise Exception(f"Invalid Index: {arg}")


    def get(self, field):
        field = self.fwf_file_like.fields[field]
        return self.line[field]

    def str(self, field, encoding):
        return str(self.get(field), encoding)

    def int(self, field):
        return int(self.get(field))

    def date(self, field, format="%Y%m%d"):
        rtn = self.str(field, None)
        rtn = datetime.strptime(rtn, format)
        return rtn

    def __repr__(self):
        return self.line
