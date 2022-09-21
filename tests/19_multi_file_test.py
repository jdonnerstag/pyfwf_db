#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name, missing-module-docstring
# pylint: disable=protected-access

from fwf_db import FWFMultiFile
from fwf_db import FWFOperator as op
from fwf_db import FWFIndexDict, FWFUniqueIndexDict
from fwf_db.core import FWFSimpleIndexBuilder
from fwf_db.core import FWFNumpyIndexBuilder
from fwf_db import FWFCythonIndexBuilder


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


class DataFile:

    FIELDSPECS = [
        {"name": "ID", "len": 5},
        {"name": "valid_from", "len": 9, "dtype": "int32", "default": 19990101},
        {"name": "valid_until", "len": 9, "dtype": "int32", "default": 21991231},
        {"name": "changed", "len": 9, "dtype": "int32"},
    ]


def test_with_statement():

    with FWFMultiFile(DataFile) as mf:
        fwf1 = mf.open_and_add(DATA_1)
        fwf2 = mf.open_and_add(DATA_2)

        assert len(mf) == 20
        assert len(mf.files) == 2
        assert mf.line_count == 20

        assert fwf1._mm is not None
        assert fwf2._mm is not None

    assert fwf1._mm is None
    assert fwf2._mm is None


def test_multi_file():
    with FWFMultiFile(DataFile) as mf:
        mf.open_and_add(DATA_1)
        mf.open_and_add(DATA_2)

        assert len(mf) == 20
        assert len(mf.files) == 2
        assert mf.line_count == 20
        assert len(list(mf)) == 20

        for idx, _ in enumerate(mf.iter_lines()):
            assert idx < 20

        for rec in mf:
            assert rec.lineno < 20
            assert rec.rooted().lineno < 10  # Each file has max 10 records

        for line in iter(mf[8:12]):     # TODO: iter(x) is only to make pylint happy. It actually works without as well.
            assert line.rooted(mf).lineno >= 8
            assert line.rooted(mf).lineno < 12

        x = mf[8:12]
        for line in iter(x):     # TODO: iter(x) is only to make pylint happy. It actually works without as well.
            assert line.rooted(mf).lineno >= 8
            assert line.rooted(mf).lineno < 12

        assert mf[0].lineno == 0
        assert mf[5].lineno == 5
        assert mf[10].lineno == 10
        assert mf[15].lineno == 15
        assert mf[19].lineno == 19
        assert len(mf[0:5]) == 5
        assert len(mf[-5:]) == 5
        assert len(mf[5:15]) == 10


def test_index():
    with FWFMultiFile(DataFile) as mf:
        mf.open_and_add(DATA_1)
        mf.open_and_add(DATA_2)

        assert len(mf) == 20
        assert len(mf.files) == 2
        assert mf.line_count == 20

        index = FWFIndexDict(mf)
        FWFSimpleIndexBuilder(index).index(mf, "ID")
        assert len(index) == 11
        for _, refs in index:
            assert 1 <= len(refs) <= 3


def test_effective_date():
    with FWFMultiFile(DataFile) as mf:
        mf.open_and_add(DATA_1)     # 10 lines
        mf.open_and_add(DATA_2)     # 10 lines

        assert len(mf) == 20        # 10 + 10 lines
        assert len(mf.files) == 2   # 2 files have been added
        assert mf.line_count == 20  # 10 + 10 lines

        # Filter all records by effective date
        filtered = mf.filter(op("changed").bytes() <= b" 20180501")
        assert len(filtered) == 10    # 5 + 5 lines from each file
        for line in filtered.iter_lines():
            line = str(line, "utf-8")
            x = int(line[0:5])
            assert x in [1, 2, 3, 4, 5, 22]

        # And now create an index
        index = FWFIndexDict(filtered)
        FWFSimpleIndexBuilder(index).index(filtered, "ID")
        assert len(index) == 6      # 1, 2, 3, 4, 5 and 22
        for _, refs in index:
            assert 1 <= len(refs) <= 2
            line = refs[0]
            line = int(str(line["ID"], "utf-8"))
            assert line in [1, 2, 3, 4, 5, 22]

            if len(refs) > 1:
                line = refs[1]
                line = int(str(line["ID"], "utf-8"))
                assert line in [1, 3, 4, 5]


def test_cython_filter():
    with FWFMultiFile(DataFile) as mf:
        mf.open_and_add(DATA_1)
        mf.open_and_add(DATA_2)

        assert len(mf) == 20
        assert len(mf.files) == 2
        assert mf.line_count == 20

        for idx, _ in enumerate(mf.iter_lines()):
            assert idx < 20

        for rec in mf:
            assert rec.lineno < 20

        for rec in iter(mf[8:12]):
            assert rec.rooted(mf).lineno >= 8
            assert rec.rooted(mf).lineno < 12

        for rec in iter(mf[8:12]):        # Note: iter(x) is only to make pylint happy. It actually works without as well.
            assert rec.rooted(mf).lineno >= 8
            assert rec.rooted(mf).lineno < 12

        assert mf[0].lineno == 0
        assert mf[5].lineno == 5
        assert mf[10].lineno == 10
        assert mf[15].lineno == 15
        assert mf[19].lineno == 19
        assert len(mf[0:5]) == 5
        assert len(mf[-5:]) == 5
        assert len(mf[5:15]) == 10


def test_cython_index():
    with FWFMultiFile(DataFile) as mf:
        fwf1 = mf.open_and_add(DATA_1)
        fwf2 = mf.open_and_add(DATA_2)

        mi = FWFIndexDict(mf)
        FWFSimpleIndexBuilder(mi).index(mf, "ID")
        assert len(mi) == 11

        assert len(mi[b"1    "]) == 2
        assert len(mi[b"22   "]) == 1

        assert mi[b"1    "][0].rooted().fwf_view == fwf1
        assert mi[b"1    "][1].rooted().fwf_view == fwf2
        assert mi[b"2    "][0].rooted().fwf_view == fwf1
        assert mi[b"22   "][0].rooted().fwf_view == fwf2

        assert mi[b"1    "][0].rooted().lineno == 0
        assert mi[b"1    "][1].rooted().lineno == 0
        assert mi[b"2    "][0].rooted().lineno == 1
        assert mi[b"22   "][0].rooted().lineno == 1

        # TODO same tests, different index implementation => restructure
        mi = FWFIndexDict(mf)
        FWFCythonIndexBuilder(mi).index(mf, "ID")
        assert len(mi) == 11

        assert len(mi[b"1    "]) == 2
        assert len(mi[b"22   "]) == 1

        assert mi[b"1    "][0].rooted().fwf_view == fwf1
        assert mi[b"1    "][1].rooted().fwf_view == fwf2
        assert mi[b"2    "][0].rooted().fwf_view == fwf1
        assert mi[b"22   "][0].rooted().fwf_view == fwf2

        assert mi[b"1    "][0].rooted().lineno == 0
        assert mi[b"1    "][1].rooted().lineno == 0
        assert mi[b"2    "][0].rooted().lineno == 1
        assert mi[b"22   "][0].rooted().lineno == 1


def test_cython_unique_index():
    with FWFMultiFile(DataFile) as mf:
        fwf1 = mf.open_and_add(DATA_1)
        fwf2 = mf.open_and_add(DATA_2)

        mi = FWFUniqueIndexDict(mf)
        FWFCythonIndexBuilder(mi).index(mf, "ID")
        assert len(mi) == 11

        assert mi[b"1    "].lineno == 10
        assert mi[b"2    "].lineno == 1
        assert mi[b"22   "].lineno == 11

        assert mi[b"1    "].rooted().fwf_view is fwf2
        assert mi[b"2    "].rooted().fwf_view is fwf1
        assert mi[b"22   "].rooted().fwf_view is fwf2

        assert mi[b"1    "].rooted().lineno == 0
        assert mi[b"2    "].rooted().lineno == 1
        assert mi[b"22   "].rooted().lineno == 1

# TODO We need multi-file tests with mem-optimized index