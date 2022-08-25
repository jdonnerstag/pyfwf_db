#!/usr/bin/env python
# encoding: utf-8


from .fwf_subset import FWFSubset
from .fwf_np_index import FWFNumpyIndex


class FWFUniqueNumpyIndex(FWFNumpyIndex):
    """A Numpy unique Index

    Especially with large files with millions of records in the index,
    a Numpy based index is (much) faster compared to pure python based on.
    """

    def _index2b(self, values):
        data = {self.data[value]: i for i, value in enumerate(values)}
        return data


    def get(self, key) -> FWFSubset:
        """Create a view with the indices associated with the index key provided"""

        assert self.data is not None

        # Numpy return an empty list [], if key is not found
        if key in self.data:
            value = self.data[key]
            return FWFSubset(self.fwfview, value, self.fwfview.fields)

        raise IndexError(f"'key' not found in Index: {key}")
