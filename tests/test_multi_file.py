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
from fwf_db.fwf_cython import FWFCython
from fwf_db.fwf_merge_index import FWFMergeIndex
from fwf_db.fwf_merge_unique_index import FWFMergeUniqueIndex
from fwf_db.fwf_line import FWFLine


DATA_1 = b"""#
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


DATA_2 = b"""#
1     19990101 21991231 20180101
22    20180101 20181231 20180201
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
    with fwf1.open(DATA_1):
        fwf2 = FWFFile(DataFile)
        with fwf2.open(DATA_2):

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
    with fwf1.open(DATA_1) as fd1, fwf2.open(DATA_2) as fd2:

        mf = FWFMultiFile()
        mf.add_file(fd1)
        mf.add_file(fd2)

        assert len(mf) == 20
        assert len(mf.files) == 2
        assert mf.lines == slice(0, 20)

        index = FWFSimpleIndex(mf).index("ID")
        assert len(index) == 11
        for key in index:
            refs = index[key]
            assert 1 <= len(refs) <= 3


def test_effective_date():
    fwf1 = FWFFile(DataFile)
    fwf2 = FWFFile(DataFile)

    with fwf1.open(DATA_1) as fd1, fwf2.open(DATA_2) as fd2:

        mf = FWFMultiFile()
        mf.add_file(fd1)
        mf.add_file(fd2)

        assert len(mf) == 20
        assert len(mf.files) == 2
        assert mf.lines == slice(0, 20)

        # Filter all records by effective date
        filtered = mf.filter(op("changed") <= b" 20180501")
        for _, line in filtered.iter_lines():
            line = line.decode("utf-8")
            x = int(line[0:5])
            assert (1 <= x <= 5) or (x == 22)

        # And now create an index
        index = FWFSimpleIndex(filtered).index("ID")
        assert len(index) == 6
        for key in index:
            refs = index[key]
            assert 1 <= len(refs) <= 3

            line = refs[0]
            line = int(line["ID"].decode("utf-8"))
            assert (1 <= x <= 5) or (x == 22)
            
            if len(refs) > 1:
                line = refs[1]
                line = int(line["ID"].decode("utf-8"))
                assert (1 <= x <= 5) or (x == 22)

        # TODO May it would be helpful to use parent consistently instead of fwfview and fwffile
        # TODO delevel is currently only available in SimpleIndex and not yet very flexible
        index = index.delevel()
        assert len(index) == 6
        for key in index:
            refs = index[key]
            assert 1 <= len(refs) <= 3

            line = refs[0]
            assert (0 <= line.lineno < 5) or (10 <= line.lineno < 15)
            
            if len(refs) > 1:
                line = refs[1]
                assert (0 <= line.lineno < 5) or (10 <= line.lineno < 15)


def test_cython_filter():

    fwf1 = FWFFile(DataFile)
    fwf2 = FWFFile(DataFile)

    with fwf1.open(DATA_1) as fd1, fwf2.open(DATA_2) as fd2:

        mf = FWFMultiFile()

        # FWFCython only works on FWFile yet (TODO)
        cf1 = FWFCython(fd1).apply(DATA_1)
        cf2 = FWFCython(fd2).apply(DATA_2)

        mf.add_file(cf1)
        mf.add_file(cf2)

        assert len(mf) == 20
        assert len(mf.files) == 2
        assert mf.lines == slice(0, 20)

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


def test_cython_index():

    fwf1 = FWFFile(DataFile)
    fwf2 = FWFFile(DataFile)

    with fwf1.open(DATA_1) as fd1, fwf2.open(DATA_2) as fd2:

        # FWFCython only works on FWFile yet (TODO)
        cf1 = FWFCython(fd1).apply(DATA_1, index="ID")
        cf2 = FWFCython(fd2).apply(DATA_2, index="ID")

        mi = FWFMergeIndex()
        mi.merge(cf1)
        mi.merge(cf2)

        assert len(mi) == 11
        assert len(mi.indices) == 2

        for key in mi:
            recs = mi[key]
            for l in recs:
                assert isinstance(l, FWFLine)

            for l, line in recs.iter_lines():
                assert isinstance(line, bytes)

        assert len(mi[b"2    "]) == 1
        assert len(mi[b"22   "]) == 1


def test_cython_unique_index():

    fwf1 = FWFFile(DataFile)
    fwf2 = FWFFile(DataFile)

    with fwf1.open(DATA_1) as fd1, fwf2.open(DATA_2) as fd2:

        # FWFCython only works on FWFile yet (TODO)
        cf1 = FWFCython(fd1).apply(DATA_1, index="ID", unique_index=True)
        cf2 = FWFCython(fd2).apply(DATA_2, index="ID", unique_index=True)

        mi = FWFMergeUniqueIndex()
        mi.merge(cf1)
        mi.merge(cf2)

        assert len(mi) == 11
        assert len(mi.indices) == 2

        for key in mi:
            line = mi[key]
            assert isinstance(line, FWFLine)

        assert mi.data[b"2    "] == (0, 1)
        assert mi.data[b"22   "] == (1, 1)



# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_multi_view()
