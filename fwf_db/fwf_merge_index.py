#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFIndexLike
from .fwf_multi_subset import FWFMultiSubset
from .fwf_file import FWFFile
from .fwf_cython import FWFCython
from .fwf_multi_file import FWFMultiFileMixin


class FWFMergeIndexException(Exception):
    pass


class FWFMergeIndex(FWFMultiFileMixin, FWFIndexLike):

    def __init__(self, filespec=None):

        self.fwfview = None
        self.field = None   # The field name to build the index
        self.data = defaultdict(list)    # dict(value -> [lineno])

        self.init_multi_file_mixin(filespec)


    def open(self, file, index):
        fwf = FWFFile(self.filespec)
        fd = fwf.open(file)

        FWFCython(fd).apply(
            index=index, 
            unique_index=False, 
            index_dict=self.data,       # Update this dict
            index_tuple=len(self.files)
        )

        self.files.append(fd)

        # No need to "manually" merge. Our little cython component 
        # has done that already
        # self.merge(cf)
       
        return self.data


    def __len__(self):
        """The number of index keys"""
        return len(self.data.keys())


    def __iter__(self):
        """Iterate over the index keys"""
        return iter(self.data.keys())


    def get(self, key):
        """Create a new view with all rows matching the index key"""
        if key in self.data:
            return FWFMultiSubset(self.files, self.data[key])


    def __contains__(self, param):
        return param in self.data
