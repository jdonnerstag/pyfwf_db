#!/usr/bin/env python
# encoding: utf-8

import numpy as np

class FWFUniqueNpBased(object):
    """A Numpy based implementation that return the unique (distinct)
    value of a field (column)

    Note: interestingly this is marginally slower then the pure python 
    implementation.
    """

    def __init__(self, fwffile, np_type):
        self.fwffile = fwffile
        self.np_type = np_type


    def unique(self, field, func=None):
        """Create a list of unique values found in 'field'.

        Use 'func' to change the value before adding it to the index, e.g. 
        str, lower, upper, int, ...
        """

        field = self.fwffile.field_from_index(field)

        # If the parent view has an optimized iterator ..
        if hasattr(self.fwffile, "iter_lines_with_field"):
            gen = self.fwffile.iter_lines_with_field(field)
        else:
            sslice = self.fwffile.fields[field]
            gen = ((i, line[sslice]) for i, line in self.fwffile.iter_lines())

        # Do we need to apply any transformations...
        if func:
            gen = ((i, func(v)) for i, v in gen)

        # Add the value if not yet present
        # Reserve all needed memory upfront
        reclen = len(self.fwffile)
        values = np.empty(reclen, dtype=self.np_type)
        for i, value in gen:
            values[i] = value

        return np.unique(values)
