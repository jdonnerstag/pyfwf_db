#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFDictIndexLike
from .fwf_line import FWFLine
from .fwf_file import FWFFile
from .fwf_cython import FWFCython
from .fwf_multi_file import FWFMultiFileMixin
from .fwf_mem_optimized_index import BytesDictWithIntListValues


class FWFMergeUniqueIndexException(Exception):
    pass


class FWFMergeUniqueIndex(FWFMultiFileMixin, FWFDictIndexLike):

    def __init__(self, filespec=None, index=None, integer_index=False):

        self.init_multi_file_mixin(filespec)
        self.init_dict_index_like(None)

        self.index = index
        self.integer_index = integer_index

        self.field = None   # The field name to build the index
        self.data = BytesDictWithIntListValues(0, unique=True)  # dict()  # dict: key -> lineno


    def open(self, file, index=None):
        fwf = FWFFile(self.filespec)
        fd = fwf.open(file)

        # Grow the underlying arrays of our specialised dict
        self.data.resize(len(fd))

        FWFCython(fd).apply(
            index=index or self.index, 
            unique_index=True, 
            integer_index=self.integer_index,
            index_dict=self.data,       # Update this dict
            index_tuple=len(self.files)
        )

        self.files.append(fd)

        return self.data


    def items(self):
        for key, (pos, lineno) in self.data.items():
            fwfview = self.files[pos]
            yield key, FWFLine(fwfview, lineno, fwfview.line_at(lineno))


    def get(self, key):
        """Create a new view with all rows matching the index key"""
        if key in self.data:
            pos, lineno = self.data[key]
            fwfview = self.files[pos]
            return FWFLine(fwfview, lineno, fwfview.line_at(lineno))


    def fwf_subset(self, fwffile, key, fields):
        return self.get(key)
