#!/usr/bin/env python
# encoding: utf-8

from typing import Callable
from deprecated import deprecated
import numpy as np

from fwf_db.fwf_index_like import FWFDictUniqueIndexLike
from .fwf_view_like import FWFViewLike


@deprecated(reason="It mainly exists to compare different implementations")
class FWFUniqueNumpyIndex(FWFDictUniqueIndexLike):
    """A Numpy unique Index

    Especially with large files with millions of records in the index,
    a Numpy based index is (much) faster compared to pure python based on.
    """

    def __init__(self,
        fwfview: FWFViewLike,
        field: int|str,
        func: None|Callable = None,
        log_progress: None|Callable = None,
        dtype = None,
        cleanup_df: None|Callable = None
    ):

        super().__init__(fwfview, field)

        self.dtype = dtype or self.fwfview.field_dtype(1)
        self.cleanup_df = cleanup_df    # TODO still not convinced this is a good idea

        self.index(func, log_progress)


    # TODO _index2 is a very bad name
    def _index2(self, gen):
        """Create the Index

        The 'field' to base the index on
        'np_type' is the Numpy dtype used to create the array that initially
        holds the index data.
        'func' to process the field data before adding it to the index, e.g.
        lower, upper, str, int, etc..
        """

        values = self._index2a(gen)

        if self.cleanup_df is not None:
            values = self.cleanup_df(values)

        self.data = groups = self._index2b(values)
        return groups


    def _index2a(self, gen):
        """Create the Index df"""

        # Create the full size index all at once => number of records
        reclen = len(self.fwfview)
        values = np.empty(reclen, dtype=self.dtype)

        for i, value in gen:
            values[i] = value

        return values


    def _index2b(self, values) -> dict:
        # I tested all sort of numpy and pandas ways, but nothing was as
        # fast as python generators. Any test needs to consider (a) how
        # long it takes to create the "index" and (b) how long it takes
        # to lookup values. For any meaningful performance indication, (a)
        # the array must have at least 10 mio entries and (b) you must
        # execute at least 1 mio lookups against the 10 mio entties.
        data = {value : i for i, value in enumerate(values)}
        return data
