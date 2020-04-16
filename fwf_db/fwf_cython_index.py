#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFDictIndexLike
from .fwf_subset import FWFSubset
from .cython import fwf_db_ext
from .fwf_simple_index import FWFSimpleIndex


class FWFCythonIndexException(Exception):
    pass


class FWFCythonIndex(FWFDictIndexLike):
    """An index implementation, that leverages Cython for performance 
    reasons. The larger the files, the larger are the performance 
    improvements.

    In case you know that your key is unique (as in Primary Key), then 
    you can further improve the performance by using e.g. 
    FWFCythonUniqueIndex.
    """

    def __init__(self, fwfview):

        self.init_dict_index_like(fwfview)
        self.field = None   # The field name to build the index
        self.data = {}      # dict(value -> [lineno])

        if getattr(self.fwfview, "mm", None) is None:
            raise FWFCythonIndexException(f"Only FWFile parent are supported with {type(self)}")


    def index(self, field, func=None, log_progress=None):
        """A convience function to create the index without generator"""

        assert log_progress is None, f"Parameter 'log_progress' is not supported with this Indexer"

        field = self.fwfview.field_from_index(field)
        self.data = fwf_db_ext.create_index(self.fwfview, field)
    
        if func is not None:
            self.data = {func(k) : v for k, v in self.data.items()}
    
        return self


    def fwf_subset(self, fwfview, key, fields):
        """Create a view based on the indices associated with the index key provided""" 
        if key in self.data:
            return FWFSubset(fwfview, self.data[key], fields)
       

    def delevel(self):
        """In case the index has been created on top of a view, then it is 
        possible to reduce the level of indirection by one.
        """
        # TODO The current implementation is rather specific and may not work with
        # TODO all kind of parents. => implement a more generic version

        data = {key : [self.fwfview.lines[i] for i in values] for key, values in self.data.items()}

        rtn = FWFSimpleIndex(self.fwfview.fwffile)
        rtn.data = data
        rtn.field = self.field

        return rtn
