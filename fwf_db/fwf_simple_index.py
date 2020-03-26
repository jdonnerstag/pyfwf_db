#!/usr/bin/env python
# encoding: utf-8

from itertools import islice

from .fwf_index_like import FWFIndexLike
from .fwf_subset import FWFSubset


class FWFSimpleIndex(FWFIndexLike):
    """A simple index implementation, based on pure python"""

    def __init__(self, fwfview):

        self.fwfview = fwfview
        self.field = None   # The field name to build the index
        self.data = {}    # dict(value -> [lineno])


    def _index(self, field, func=None, chunksize=None):

        # Determine the slice information for the field
        fields = self.fwfview.fields
        if isinstance(field, int):
            field = next(islice(fields.keys(), field, None))

        sslice = fields[field]

        # This will contain the actually index info
        self.data = values = {}     # dict(name => [<indices])

        for i, line in self.fwfview.iter_lines():
            value = line[sslice]
            if func:
                value = func(value)

            rtn = values.get(value, [i])
            if value in values:
                rtn.append(i)
            else:
                values[value] = rtn

            if chunksize:
                yield i
                
        return self


    def __len__(self):
        """The number of index keys"""
        return len(self.data.keys())


    def __iter__(self):
        """Iterate over the index keys"""
        return iter(self.data.keys())


    def fwf_subset(self, fwffile, key, fields):
        """Create a view based on the indices associated with the index key provided""" 
        if key in self.data:
            return FWFSubset(fwffile, self.data[key], fields)


    def __contains__(self, param):
        return param in self.data
        