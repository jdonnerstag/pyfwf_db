#!/usr/bin/env python
# encoding: utf-8

import sys
from datetime import datetime


class FWFLine(object):
    """A dictory like convinience class to access the fields within a 
    line. Access is similar to dict() with get(), [], keys, in, ...
    """ 

    def __init__(self, fwf_file_like, lineno, line):
        assert fwf_file_like is not None
        #assert isinstance(lineno, int)     Numpy provides a int-like object

        self.fwf_file_like = fwf_file_like     
        self.lineno = lineno
        self.line = line


    def __getitem__(self, arg):
        """Get a field or range of bytes from the line.

        string: representing a field name, get the data associated with it.
        int: get the byte at the position
        slice: get a bytearray for that slice
        """
        if isinstance(arg, str):
            return self.get(arg)
        elif isinstance(arg, (int, slice)):
            return self.line[arg]
        else:
            raise Exception(f"Invalid Index: {arg}")


    def get(self, field, default=None):
        """Get the binary data for the field""" 
        field = self.fwf_file_like.fields[field]
        rtn = self.line[field]
        if rtn:
            return rtn

        return default
            

    def str(self, field, encoding=None):
        """Get the data for the field converted into a string, optionally 
        applying an encoding
        """
        encoding = encoding or sys.getdefaultencoding()
        return str(self.get(field), encoding)


    def int(self, field):
        """Get the data for the field converted into an int"""
        return int(self.get(field))


    def date(self, field, format="%Y%m%d"):
        """Get the data for the field converted into an datetime object
        applying the 'format'
        """
        rtn = self.str(field, None)
        rtn = datetime.strptime(rtn, format)
        return rtn


    def __contains__(self, key):
        """suppot pythons 'in' operator"""
        return key in self.fwf_file_like.fields


    def keys(self): 
        """Like dict's keys() method, return all field names"""
        return self.fwf_file_like.fields.keys()


    def items(self):
        """Like dict's items(), return field name and value tuples"""
        for key in self.keys():
            rtn = self.get(key)
            yield (key, rtn)


    def to_dict(self):
        """Provide the line as dict""" 
        return {k: v for k, v in self.items()}


    def __repr__(self):
        return self.line
