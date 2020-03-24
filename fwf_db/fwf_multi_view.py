#!/usr/bin/env python
# encoding: utf-8

"""
"""

from itertools import islice

from fwf_db.fwf_view import FWFView


class FWFMultiView(object):
    """    """

    def __init__(self, columns=None):

        self.columns = columns
        self.files = []
        self.lines = None


    def add_file(self, fwf_view):
        assert fwf_view is not None

        self.files.append(fwf_view)
        self.lines = slice(0, sum(len(x) for x in self.files))

        if self.columns is None:
            self.columns = fwf_view.columns


    def remove_file(self, fwf_view):
        if fwf_view is None:
            return 

        self.files = self.files.remove(fwf_view)
        self.lines = slice(0, sum(len(x) for x in self.files))


    def from_slices(self, files, slices):
        for idx, start, stop in slices:
            file = files[idx][start:stop]
            self.add_file(file)

        return self


    def __len__(self):
        return self.lines.stop - self.lines.start


    # TODO same as in FWFView
    def add_slices(self, a, b):
        parent_size = a.stop - a.start
        b = self.normalize_slice(parent_size, b)
        b = slice(a.start + b.start, a.start + b.stop)  
        b = self.normalize_slice(parent_size, b)
        return b


    # TODO same as in FWFView
    def normalize_slice(self, parent_size, xslice):
        start = xslice.start
        stop = xslice.stop

        if start is None:
            start = 0
        elif start < 0:
            start = parent_size + start
            if stop == 0:
                stop = None	# == end of file
        
        if (start < 0) or (start >= parent_size):
            raise Exception(
                f"Invalid start index {start} for slice {xslice}. "
                f"Parent size: {parent_size}")

        if stop is None:
            stop = parent_size
        elif stop < 0:
            stop = parent_size + stop + 1
        
        if (stop < 0) or (stop > parent_size):
            raise Exception(
                f"Invalid stop index {stop} for slice {xslice}. "
                f"Parent size: {parent_size}")

        if stop < start:
            raise Exception(
                f"Invalid slice: start <= stop: {start} <= {stop} for "
                f"slice {xslice} and parent size {parent_size}")

        return slice(start, stop)


    def determine_fwf_table_slices(self, start, stop):
        rtn = []
        start_pos = 0
        for i, file in enumerate(self.files):
            flen = len(file)
            ffrom = start_pos
            fto = ffrom + flen

            if (start >= ffrom) and (stop <= fto):
                rtn.append((i, start - ffrom, stop - ffrom))
            elif (start >= ffrom) and (start < fto) and (stop > fto):
                rtn.append((i, start - ffrom, flen))
            elif (start < ffrom) and (stop >= ffrom) and (stop <= fto):
                rtn.append((i, 0, stop - ffrom))
            elif (start < ffrom) and (stop > fto):
                rtn.append((i, 0, flen))

            if (fto >= stop):
                break

            start_pos = fto

        return rtn


    def determine_fwf_table_index(self, index):
        start_pos = 0
        for i, file in enumerate(self.files):
            flen = len(file)
            ffrom = start_pos
            fto = ffrom + flen

            if (index >= ffrom) and (index <= fto):
                return (i, index - ffrom, index - ffrom + 1)

            start_pos = fto


    def iloc(self, start, end=None, columns=None):
        if columns:
            columns = [name for name in columns if name in self.columns]
        else:
            columns = self.columns

        if end is None:
            end = start + 1

        xslice = self.normalize_slice(len(self), slice(start, end))   

        slices = self.determine_fwf_table_slices(xslice.start, xslice.stop)
        multiview = FWFMultiView().from_slices(self.files, slices)
        return multiview


    # TODO same as in FWFView
    def __getitem__(self, args):
        (row_idx, cols) = args if isinstance(args, tuple) else (args, None)

        if isinstance(row_idx, slice):
            return self.iloc(row_idx.start, row_idx.stop, cols)
        elif isinstance(row_idx, int):
            return self.iloc(row_idx, None, cols)


    # TODO same as in FWFView
    def __iter__(self):
        return self.iter()


    def iter_lines_with_index(self, index):
        for i in index:
            (file, start, _) = self.determine_fwf_table_index(i)
            line = self.files[file].iloc(start)
            yield i, file, line


    def iter_lines_with_slices(self, xslice):
        slices = self.determine_fwf_table_slices(xslice.start, xslice.stop)
        count = 0
        for file, start, stop in slices:
            for _, line in self.files[file].iter_lines(slice(start, stop)):
                yield count, file, line
                count += 1


    # TODO same as in FWFView
    def iter_lines(self):
        if isinstance(self.lines, slice):
            yield from self.iter_lines_with_slices(self.lines)
        elif isinstance(self.lines, list):
            yield from self.iter_lines_with_index(self.lines)
        elif isinstance(self.lines, int):
            yield from self.iter_lines_with_index([self.lines])
        else:
            raise Exception(f"Invalid range: {self.lines}")


    def iter(self):
        for idx, file_idx, line in self.iter_lines():
            rtn = [line[v] for v in self.columns.values()]
            yield (rtn, idx, file_idx)


    def get_raw_value(self, i, line, field):
        return self.files[i].get_raw_value(line, field)


    def get_value(self, i, line, field):
        return self.files[i].get_value(line, field)


    # TODO same as in FWFView
    def filter_by_line(self, func):
        rtn = [i for i, rec in self.iter_lines() if func(rec)]
        return FWFView(self, rtn, self.columns)                


    # TODO same as in FWFView
    def filter_by_field(self, field, func):
        sslice = self.columns[field]

        if callable(func):
            rtn = [i for i, rec in self.iter_lines() if func(rec[sslice])]
        else:
            rtn = [i for i, rec in self.iter_lines() if rec[sslice] == func]

        return FWFView(self, rtn, self.columns)                
        

    # TODO same as in FWFView
    def unique(self, field, func=None):
        values = set()
        xslice = self.columns[field]
        for _, line in self.iter_lines():
            value = line[xslice].tobytes()
            if func:
                value = func(value)
            values.add(value)

        return values


    # TODO same as in FWFView
    def to_pandas(self):
        raise Exception("Not yet implemented")
