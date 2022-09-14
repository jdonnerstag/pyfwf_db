#!/usr/bin/env python
# encoding: utf-8

from typing import Callable, Iterator
from deprecated import deprecated
import numpy as np

from .fwf_index_like import FWFIndexBuilder, FWFIndexLike
from .fwf_view_like import FWFViewLike


@deprecated(reason="It mainly exists to compare different implementations")
class FWFNumpyIndexBuilder(FWFIndexBuilder):
    """A Numpy based Index builder

    Especially with large files with millions of records in the index,
    a Numpy based index is (much) faster compared to pure python based on.
    """

    def __init__(self, data: FWFIndexLike, dtype = None):
        self.data = data
        self.dtype = dtype


    def index(self, fwfview: FWFViewLike, field: int|str, **kwargs):
        if "dtype" not in kwargs:
            kwargs["dtype"] = self.dtype or fwfview.field_dtype(1)

        super().index(fwfview, field, **kwargs)


    def create_index_from_generator(self, fwfview: FWFViewLike, gen: Iterator[bytes], **kwargs) -> None:
        """Create the Index

        The 'field' to base the index on
        'np_type' is the Numpy dtype used to create the array that initially
        holds the index data.
        'func' to process the field data before adding it to the index, e.g.
        lower, upper, str, int, etc..
        """

        # Create the full size index all at once => number of records
        dtype = kwargs["dtype"]
        line_count = len(fwfview)
        values = np.empty(line_count, dtype=dtype)

        # Note: I'm wondering if that safes memory: store the data in a numpy array
        # and add it later to the dict. It's only an improvement, if the data
        #  remain in numpy and are merely references.
        for i, value in enumerate(gen):
            values[i] = value

        # I tested all sort of numpy and pandas ways, but nothing was as
        # fast as python generators. Any test needs to consider (a) how
        # long it takes to create the "index" and (b) how long it takes
        # to lookup values. For any meaningful performance indication, (a)
        # the array must have at least 10 mio entries and (b) you must
        # execute at least 1 mio lookups against the 10 mio entties.
        for i, value in enumerate(values):
            self.data[value] = i
