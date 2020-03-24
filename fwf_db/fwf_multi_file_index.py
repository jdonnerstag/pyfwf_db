#!/usr/bin/env python
# encoding: utf-8

"""An Index across multiple files.

You may receive a file with updated content every day. To avoid the need
to concatenate the files first, this index implementation allows to create
an index across multiple files, and access the content from all files, as
if it were one file.
"""

from itertools import islice


class FWFSMultiFileIndex(object):
    def __init__(self, fwf_view):
        assert fwf_view

        self.fwf_view = fwf_view
        self.column = None
        self.idx = None
        self.files = [fwf_view.fwf_table.file] 

        self.column = None
        self.func = None
        self.progress_log = None


    def add_file(self, file):
        index = len(self.files)
        self.files.append(file)
        self.index(self.column, self.func, self.progress_log, index)
        return self


    def index(self, column, func=None, progress_log=None, index=0):
        self.column = column
        self.func = func
        self.progress_log = progress_log

        self.idx = idx = {}  # key => (file_id, lineno) 

        cols = self.fwf_view.columns
        if isinstance(column, str):
            xslice = cols[column]
        elif isinstance(column, int):
            xslice = cols[next(islice(cols, column, None))]
        else:
            raise Exception(f"column must be either string or int: {column}")

        for i, line in self.fwf_view.iter_lines():
            value = line[xslice]
            if func:
                value = func(value)
            
            v = idx.get(value, None)
            if not v:
                idx[value] = [(index, i)]
            else:
                v.append((index, i))

            if progress_log:
                progress_log(i)
                
        return self


    def __contains__(self, key):
        return key in self.idx


    def __getitem__(self, key):
        return self.loc(key)


    def loc(self, key):
        from fwf_db.fwf_view import FWFView

        rtn = self.idx.get(key, None)
        if rtn is not None:
            view = self.fwf_view
            tbl = self.fwf_view.fwf_table

            from .fwf_multi_file_view import FWFMultiFileView
            return FWFMultiFileView(tbl, self.files, rtn, view.columns)


    def __len__(self):
        return len(self.idx)
