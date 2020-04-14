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


class FWFMergeUniqueIndex(FWFIndexLike, FWFMultiFileMixin):

    def __init__(self, filespec=None):

        self.fwfview = None
        self.field = None   # The field name to build the index
        self.indices = []
        self.data = dict()

        self.init_multi_file_mixin(filespec)


    def open(self, file, index):
        fwf = FWFFile(self.filespec)
        fd = fwf.open(file)
        self.files.append(fd)

        cf = FWFCython(fd).apply(index=index, unique_index=True)
        self.merge(cf)
        return cf


    def merge(self, index):

        idx = len(self.indices)

        if getattr(index, "data", None) is None:
            raise FWFMergeUniqueIndexException(
                f"'index' must be of type by an Index with 'data' attribute")

        self.indices.append(index)
        
        if not self.fwfview:
            self.fwfview = index.fwfview

        for k, v in index.data.items():
            self.data[k] = (idx, v)

        return self


    def __len__(self):
        """The number of index keys"""
        return len(self.data.keys())


    def __iter__(self):
        """Iterate over the index keys"""
        return iter(self.data.keys())


    def fwf_subset(self, fwfview, key, fields):
        if key in self.data:
            pos, lineno = self.data[key]
            fwfview = self.indices[pos].fwfview
            return FWFLine(fwfview, lineno, fwfview.line_at(lineno))


    def __contains__(self, param):
        return param in self.data
