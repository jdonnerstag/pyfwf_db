#!/usr/bin/env python
# encoding: utf-8

import numpy as np

class FWFUniqueNpBased(object):
    """A Numpy based implementation that return the unique (distinct)
    value of a field (column)
    """

    def __init__(self, fwffile, np_type):
        self.fwffile = fwffile
        self.np_type = np_type


    def unique(self, field, func=None):
        """Create a list of unique values found in 'field'.

        Use 'func' to change the value before adding it to the index, e.g. 
        str, lower, upper, int, ...
        """

        reclen = len(self.fwffile)
        values = np.empty(reclen, dtype=self.np_type)

        sslice = self.fwffile.fields[field]

        for i, line in self.fwffile.iter_lines():
            value = line[sslice]
            if func:
                value = func(value)
                
            values[i] = value

        return np.unique(values)
