#!/usr/bin/env python
# encoding: utf-8

"""A very simple FWFIndex implementation. 

It is using a dict() to maintain the index, which has the nice 
side-effect that it does groupby as well.
"""

from itertools import islice

from .fwf_index_view import FWFIndexView


class FWFSimpleIndex(object):
    def __init__(self, parent):
        assert parent

        self.parent = parent
        self.column = None
        self.idx = None


    def index(self, column, func=None, progress_log=None):
        self.column = column
        self.idx = idx = {}

        cols = self.parent.columns
        if isinstance(column, str):
            xslice = cols[column]
        elif isinstance(column, int):
            xslice = cols[next(islice(cols, column, None))]
        else:
            raise Exception(f"column must be either string or int: {column}")

        for i, line in self.parent.iter_lines():
            value = line[xslice]
            if func:
                value = func(value)
            
            v = idx.get(value, None)
            if not v:
                idx[value] = [i]
            else:
                v.append(i)

            if progress_log:
                progress_log(i)
                
        return self


    def __contains__(self, key):
        return key in self.idx


    def __getitem__(self, key):
        return self.loc(key)


    def loc(self, key):
        lines = self.idx.get(key, None)
        if lines is not None:
            return FWFIndexView(self.parent, lines, None)


    def __len__(self):
        return len(self.idx)
