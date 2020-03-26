#!/usr/bin/env python
# encoding: utf-8



class FWFUnique(object):
    def __init__(self, fwffile):
        self.fwffile = fwffile


    def unique(self, field, func=None):
        """Create a list of unique values found in 'field'.

        Use 'func' to change the value before adding it to the index, e.g. 
        str, lower, upper, int, ...
        """

        sslice = self.fwffile.fields[field]
        values = set()
        for _, line in self.fwffile.iter_lines():
            value = line[sslice]
            if func:
                value = func(value)
            values.add(value)

        return values
