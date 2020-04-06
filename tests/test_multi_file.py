#!/usr/bin/env python
# encoding: utf-8

import pytest

import os
import sys
import io
import numpy as np

from fwf_db import FWFFile, FWFSimpleIndex, FWFMultiFile, FWFUnique
from fwf_db.fwf_unique_np_based import FWFUniqueNpBased
from fwf_db.fwf_index_np_based import FWFIndexNumpyBased
from fwf_db.fwf_operator import FWFOperator as op


DATA = b"""#
1     19990101 21991231 20180101
2     20180101 20181231 20180201
3     20180201 20180231 20180301
4     20180301 21991231 20180401
5     20180415 20180931 20180501
6     20180501 20190331 20180601
7     20180501 21991231 20180701
8     20180505 20181001 20180801
9     20180515 20181231 20180901
10    20180531 20180601 20181001
"""


class DataFile(object):

    FIELDSPECS = [
        {"name": "ID", "len": 5},
        {"name": "valid_from", "len": 9, "dtype": "int32", "default": 19990101},
        {"name": "valid_until", "len": 9, "dtype": "int32", "default": 21991231},
        {"name": "changed", "len": 9, "dtype": "int32"},
    ]


def test_multi_view():
    fwf1 = FWFFile(DataFile)
    with fwf1.open(DATA):
        fwf2 = FWFFile(DataFile)
        with fwf2.open(DATA):

            mf = FWFMultiFile()
            mf.add_file(fwf1)
            mf.add_file(fwf2)

            assert len(mf) == 20
            assert len(mf.files) == 2
            assert mf.lines == slice(0, 20)

            assert len(list(mf)) == 20

            for idx, _ in mf.iter_lines():
                assert idx < 20

            for rec in mf:
                assert rec.lineno < 20

            for idx, _ in mf[8:12].iter_lines():
                assert idx >= 8
                assert idx < 12

            for rec in mf[8:12]:
                assert rec.lineno >= 8
                assert rec.lineno < 12

            assert mf[0].lineno == 0
            assert mf[5].lineno == 5
            assert mf[10].lineno == 10
            assert mf[15].lineno == 15
            assert mf[19].lineno == 19
            assert len(mf[0:5]) == 5
            assert len(mf[-5:]) == 5
            assert len(mf[5:15]) == 10


def test_index():
    fwf1 = FWFFile(DataFile)
    fwf2 = FWFFile(DataFile)

    # TODO May be multi-file can auto-open and close them??
    # TODO and multi file becomes a contextmanager?
    with fwf1.open(DATA) as fd1, fwf2.open(DATA) as fd2:

        mf = FWFMultiFile()
        mf.add_file(fd1)
        mf.add_file(fd2)

        assert len(mf) == 20
        assert len(mf.files) == 2
        assert mf.lines == slice(0, 20)

        index = FWFSimpleIndex(mf).index("ID")
        assert len(index) == 10
        for key in index:
            refs = index[key]
            assert len(refs) == 2


def test_effective_date():
    fwf1 = FWFFile(DataFile)
    fwf2 = FWFFile(DataFile)

    with fwf1.open(DATA) as fd1, fwf2.open(DATA) as fd2:

        mf = FWFMultiFile()
        mf.add_file(fd1)
        mf.add_file(fd2)

        assert len(mf) == 20
        assert len(mf.files) == 2
        assert mf.lines == slice(0, 20)

        # Filter all records by effective date
        filtered = mf.filter(op("changed") <= b" 20180501")
        for i, line in filtered.iter_lines():
            line = line.decode("utf-8")
            x = int(line[0:5])
            assert 1 <= x <= 5

        # And now create an index
        index = FWFSimpleIndex(filtered).index("ID")
        assert len(index) == 5
        for key in index:
            refs = index[key]
            assert len(refs) == 2

            line = refs[0]
            line = int(line["ID"].decode("utf-8"))
            assert 1 <= x <= 5
            
            line = refs[1]
            line = int(line["ID"].decode("utf-8"))
            assert 1 <= x <= 5

        # TODO May it would be helpful to use parent consistently instead of fwfview and fwffile
        # TODO delevel is currently only available in SimpleIndex and not yet very flexible
        index = index.delevel()
        assert len(index) == 5
        for key in index:
            refs = index[key]
            assert len(refs) == 2

            line = refs[0]
            assert (0 <= line.lineno < 5) or (10 <= line.lineno < 15)
            
            line = refs[1]
            assert (0 <= line.lineno < 5) or (10 <= line.lineno < 15)


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_multi_view()
