#!/usr/bin/env python
# encoding: utf-8

import numpy as np

from .fwf_base_mixin import FWFBaseMixin


class FWFUniqueNpBased(FWFBaseMixin):
    """A Numpy based implementation that return the unique (distinct)
    value of a field (column)

    Note: interestingly this is marginally slower then the pure python 
    implementation.
    """

    def __init__(self, fwffile):
        self.fwffile = fwffile


    def unique(self, field, dtype=None, func=None):
        """Create a list of unique values found in 'field'.

        Use 'func' to change the value before adding it to the index, e.g. 
        str, lower, upper, int, ...
        """

        if dtype is None:
            dtype = self.fwffile.field_dtype(1)

        gen = self._index1(self.fwffile, field, func)

        # Add the value if not yet present
        # Reserve all needed memory upfront
        reclen = len(self.fwffile)
        values = np.empty(reclen, dtype=dtype)
        for i, value in gen:
            values[i] = value

        return np.unique(values)
