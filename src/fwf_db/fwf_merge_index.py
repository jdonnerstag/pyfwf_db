#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFDictIndexLike
from .fwf_multi_subset import FWFMultiSubset
from .fwf_file import FWFFile
from ._cython import fwf_db_cython
from .fwf_multi_file import FWFMultiFileMixin
from .fwf_mem_optimized_index import BytesDictWithIntListValues


class FWFMergeIndex(FWFMultiFileMixin, FWFDictIndexLike):

    def __init__(self, filespec=None, index=None, integer_index=False):

        FWFMultiFileMixin.__init__(self, filespec)
        FWFDictIndexLike.__init__(self, None)

        self.index = index
        self.integer_index = integer_index

        self.field = None   # The field name to build the index

        self.data = BytesDictWithIntListValues(0)  # # dict(value -> [lineno])
        # self.data = defaultdict(list)   # dict(value -> [lineno])


    def open(self, file, index=None):
        fwf = FWFFile(self.filespec)
        fd = fwf.open(file)

        # Grow the underlying arrays of our specialised dict
        if isinstance(self.data, BytesDictWithIntListValues):
            self.data.resize(len(fd))

        FWFCython(fd).apply(
            index=index or self.index,
            unique_index=False,
            integer_index=self.integer_index,
            index_dict=self.data,       # Update this dict
            index_tuple=len(self.files)
        )

        self.files.append(fd)

        return self.data


    def items(self):
        for key, value in self.data.items():
            yield key, FWFMultiSubset(self.files, value)


    def get(self, key):
        """Create a new view with all rows matching the index key"""
        if key in self.data:
            return FWFMultiSubset(self.files, self.data[key])


    def fwf_subset(self, fwffile, key, fields):
        return self.get(key)


    def iloc(self, idx):
        for i, (_, data) in enumerate(self.data.items()):
            if i >= idx:
                return FWFMultiSubset(self.files, data)
