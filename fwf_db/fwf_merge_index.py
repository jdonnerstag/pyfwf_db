#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFDictIndexLike
from .fwf_multi_subset import FWFMultiSubset
from .fwf_file import FWFFile
from .fwf_cython import FWFCython
from .fwf_multi_file import FWFMultiFileMixin
from .fwf_mem_optimized_index import BytesDictWithIntListValues


class FWFMergeIndexException(Exception):
    pass


class FWFMergeIndex(FWFMultiFileMixin, FWFDictIndexLike):

    def __init__(self, filespec=None, index=None, integer_index=False):

        self.init_multi_file_mixin(filespec)
        self.init_dict_index_like(None)

        self.index = index
        self.integer_index = integer_index

        self.field = None               # The field name to build the index
        self.data = BytesDictWithIntListValues(0)  # defaultdict(list)   # dict(value -> [lineno])


    def open(self, file, index=None):
        fwf = FWFFile(self.filespec)
        fd = fwf.open(file)

        # Grow the underlying arrays of our specialised dict
        self.data.resize(len(fd))

        FWFCython(fd).apply(
            index=index or self.index, 
            unique_index=True,  # False,   because of BytesDictWithIntListValues
            integer_index=self.integer_index,
            index_dict=self.data,       # Update this dict
            index_tuple=len(self.files)
        )

        self.files.append(fd)

        # No need to "manually" merge. Our little cython component 
        # has done that already
        # self.merge(cf)
       
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
