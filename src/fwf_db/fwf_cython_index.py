#!/usr/bin/env python
# encoding: utf-8

from typing import Callable
from ._cython import fwf_db_cython
from .fwf_index_like import FWFDictIndexLike
from .fwf_subset import FWFSubset
from .fwf_simple_index import FWFSimpleIndex


class FWFCythonIndexException(Exception):
    ''' FWFCythonIndexException '''


class FWFCythonIndex(FWFDictIndexLike):
    """An index implementation, that leverages Cython for performance
    reasons. The larger the files, the larger are the performance gains.

    In case you know that your key is unique (as in Primary Key), then
    you can further improve the performance by using e.g. FWFCythonUniqueIndex.
    """

    def __init__(self, fwfview):

        self.init_dict_index_like(fwfview)  # TODO Why not normal init()
        self.field = None   # The field name to build the index
        self.data = {}      # dict(value -> [lineno])

        if getattr(self.fwfview, "mm", None) is None:
            raise FWFCythonIndexException(f"Only FWFile parent are supported with {type(self)}")


    def index(self, field, func=None, log_progress: None|Callable = None):
        """A convience function to create the index without generator"""

        assert log_progress is None, "Parameter 'log_progress' is not supported with this Indexer"

        field = self.fwfview.field_from_index(field)
        self.data = fwf_db_cython.create_index(self.fwfview, field)

        if func is not None:
            self.data = {func(k) : v for k, v in self.data.items()}

        return self


    def get(self, key) -> FWFSubset:
        """Create a view based on the indices associated with the index key provided"""

        # self.data is a defaultdict, hence the additional 'in' test
        if key in self.data:
            return FWFSubset(self.fwfview, self.data[key], self.fwfview.fields)

        raise IndexError(f"'key' not found in Index: {key}")


    def delevel(self) -> FWFSimpleIndex:
        """In case the index has been created on top of a view, then it is
        possible to reduce the level of indirection by one.
        """
        # TODO The current implementation is rather specific and may not work with
        # TODO all kind of parents. => implement a more generic version
        # TODO The current approach creates a new dict, which may consume lots of memory.
        #      I wonder whether an in-place modification would be possible?

        data = {key : [self.fwfview.lines[i] for i in values] for key, values in self.data.items()}

        rtn = FWFSimpleIndex(self.fwfview.fwffile)
        rtn.data = data
        rtn.field = self.field

        return rtn
