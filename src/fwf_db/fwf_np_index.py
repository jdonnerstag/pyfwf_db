#!/usr/bin/env python
# encoding: utf-8

from typing import Callable, Iterator
from collections import defaultdict
from deprecated import deprecated
import numpy as np

from .fwf_index_like import FWFDictIndexLike
from .fwf_view_like import FWFViewLike


@deprecated(reason="It mainly exists to compare different implementations")
class FWFNumpyIndex(FWFDictIndexLike):
    """A Numpy based Index

    Especially with large files with millions of records in the index,
    a Numpy based index is (much) faster compared to pure python based on.
    """

    def __init__(self,
        fwfview: FWFViewLike,
        field: int|str,
        func: None|Callable=None,
        log_progress: None|Callable = None,
        dtype=None,
        cleanup_df: None|Callable =None):

        super().__init__(fwfview, field)

        self.dtype = dtype or self.fwfview.field_dtype(1)
        self.cleanup_df = cleanup_df

        self.index(func, log_progress)


    # TODO _index2 is a very bad name
    def _index2(self, gen: Iterator[bytes]):
        """Create the Index

        The 'field' to base the index on
        'np_type' is the Numpy dtype used to create the array that initially
        holds the index data.
        'func' to process the field data before adding it to the index, e.g.
        lower, upper, str, int, etc..
        """

        # Create the full size index all at once => number of records
        line_count = len(self.fwfview)
        values = np.empty(line_count, dtype=self.dtype)

        # Note: I'm wondering if that safes memory: store the data in a numpy array
        # and add it later to the dict. It's only an improvement, if the data
        #  remain in numpy and are merely references.
        for i, value in enumerate(gen):
            values[i] = value

        if self.cleanup_df is not None:
            values = self.cleanup_df(values)

        # I tested all sort of numpy and pandas ways, but nothing was as
        # fast as python generators. Any test needs to consider (a) how
        # long it takes to create the "index" and (b) how long it takes
        # to lookup values. For any meaningful performance indication, (a)
        # the array must have at least 10 mio entries and (b) you must
        # execute at least 1 mio lookups against the 10 mio entties.
        data = defaultdict(list)
        all(data[value].append(i) or True for i, value in enumerate(values))

        self.data = data
