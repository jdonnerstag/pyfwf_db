#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFIndexLike
from .fwf_line import FWFLine
from .fwf_file import FWFFile
from .fwf_cython import FWFCython
from .fwf_multi_file import FWFMultiFileMixin


class FWFMergeUniqueIndexException(Exception):
    pass


class FWFMergeUniqueIndex(FWFMultiFileMixin, FWFIndexLike):

    def __init__(self, filespec=None):

        self.fwfview = None
        self.field = None   # The field name to build the index
        self.data = dict()

        self.init_multi_file_mixin(filespec)


    def open(self, file, index):
        fwf = FWFFile(self.filespec)
        fd = fwf.open(file)

        FWFCython(fd).apply(
            index=index, 
            unique_index=True, 
            index_dict=self.data,       # Update this dict
            index_tuple=len(self.files)
        )

        self.files.append(fd)

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
            pos, lineno = self.data[key]
            fwfview = self.files[pos]
            return FWFLine(fwfview, lineno, fwfview.line_at(lineno))


    def __contains__(self, param):
        return param in self.data
