#!/usr/bin/env python
# encoding: utf-8

from collections import defaultdict
from itertools import islice

from .fwf_index_like import FWFDictIndexLike
from .fwf_line import FWFLine
from .fwf_file import FWFFile
from ._cython import fwf_db_cython
from .fwf_multi_file import FWFMultiFileMixin


class FWFMergeUniqueIndex(FWFMultiFileMixin, FWFDictIndexLike):

    def __init__(self, filespec=None, index=None, integer_index=False):

        FWFMultiFileMixin.__init__(self, filespec)
        FWFDictIndexLike.__init__(self, None)

        self.index = index
        self.integer_index = integer_index

        self.field = None   # The field name to build the index
        self.data = dict()  # dict: key -> lineno


    def open(self, file, index=None):
        fwf = FWFFile(self.filespec)
        fd = fwf.open(file)

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
