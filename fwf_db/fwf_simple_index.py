#!/usr/bin/env python
# encoding: utf-8

from itertools import islice

from .fwf_index_like import FWFIndexLike
from .fwf_subset import FWFSubset


class FWFSimpleIndex(FWFIndexLike):

    def __init__(self, fwfview):
        self.fwfview = fwfview
        self.field = None
        self.data = {}    # dict(value -> [lineno])


    def index(self, field, func=None, progress_log=None):
        fields = self.fwfview.fields
        if isinstance(field, int):
            field = next(islice(fields.keys(), field, None))

        sslice = fields[field]
        self.data = values = {}
        for i, line in self.fwfview.iter_lines():
            value = line[sslice]
            if func:
                value = func(value)

            rtn = values.get(value, [i])
            if value in values:
                rtn.append(i)
            else:
                values[value] = rtn

            if progress_log:
                progress_log(i)
                
        return self


    def __len__(self):
        return len(self.data.keys())


    def __iter__(self):     
        return iter(self.data.keys())


    def fwf_subset(self, fwffile, key, fields):
        if key in self.data:
            return FWFSubset(fwffile, self.data[key], fields)
