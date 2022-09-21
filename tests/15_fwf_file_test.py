#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name, missing-module-docstring
# pylint: disable=protected-access

# Current version of pylint not yet working well with python type hints and is causing plenty false positiv.
# pylint: disable=not-an-iterable, unsubscriptable-object

from typing import Iterable

import pytest

from fwf_db import FWFFile
from fwf_db import FWFLine
from fwf_db import FWFRegion
from fwf_db import FWFSubset
from fwf_db import fwf_open


DATA = b"""# My comment test
US       AR19570526Fbe56008be36eDianne Mcintosh         Whatever    Medic        #
US       MI19940213M706a6e0afc3dRosalyn Clark           Whatever    Comedian     #
US       WI19510403M451ed630accbShirley Gray            Whatever    Comedian     #
US       MD20110508F7e5cd7324f38Georgia Frank           Whatever    Comedian     #
US       PA19930404Mecc7f17c16a6Virginia Lambert        Whatever    Shark tammer #
US       VT19770319Fd2bd88100facRichard Botto           Whatever    Time traveler#
US       OK19910917F9c704139a6e3Alberto Giel            Whatever    Student      #
US       NV20120604F5f02187599d7Mildred Henke           Whatever    Super hero   #
US       AR19820125Fcf54b2eb5219Marc Kidd               Whatever    Medic        #
US       ME20080503F0f51da89a299Kelly Crose             Whatever    Comedian     #
"""


class HumanFile:

    FIELDSPECS = [
        {"name": "location", "len": 9},
        {"name": "state", "len": 2},
        {"name": "birthday", "len": 8},
        {"name": "gender", "len": 1},
        {"name": "name", "len": 36},
        {"name": "universe", "len": 12},
        {"name": "profession", "len": 13},
        {"name": "dummy", "len": 1},
    ]


def test_open():
    # We need both option:
    # 1) using a 'with' statement and automatically close again
    # 2) manually open and close it

    with fwf_open(HumanFile, DATA) as fwf:
        assert isinstance(fwf, FWFFile)
        assert fwf._mm is not None

    assert fwf._mm is None

    fd = fwf.open(DATA)
    assert fd is not None
    assert fd is fwf
    assert fd._mm is not None

    fd.close()
    assert fd._mm is None
    assert fwf._mm is None


def test_bytes_input():
    with fwf_open(HumanFile, DATA) as fwf:
        assert isinstance(fwf, FWFFile)
        assert fwf.encoding is None
        assert len(fwf.fields) == 8
        assert fwf.fwidth == 83
        assert fwf._fd is None
        assert fwf._mm is not None
        assert fwf.start_pos == 18
        assert fwf.line_count == 10
        assert len(fwf) == 10


def test_file_input():
    with fwf_open(HumanFile, "./example_data/humans.txt") as fwf:
        assert isinstance(fwf, FWFFile)
        assert fwf.encoding is None
        assert len(fwf.fields) == 8
        assert fwf.fwidth == 83
        assert fwf._fd is not None
        assert fwf._mm is not None
        assert fwf.line_count == 10012
        assert fwf.start_pos == 0
        assert len(fwf) == 10012


def test_table_iter():
    with fwf_open(HumanFile, DATA) as fwf:
        for rec in fwf:
            assert rec
            assert rec.line
            assert rec.lineno < 10 # we have 10 rows in our test data

        # Same with full file slice
        x = fwf[:]
        assert isinstance(x, Iterable)
        #for rec in fwf[:]:
        for rec in x:
            assert rec
            assert rec.line
            assert rec.lineno < 10 # we have 10 rows in our test data


def test_table_line_selector():
    with fwf_open(HumanFile, DATA) as fwf:
        rec1 = fwf.line_at(0)
        assert isinstance(rec1, FWFLine)
        assert rec1.lineno == 0
        assert rec1["birthday"] == b"19570526"

        rec1 = fwf[0]   # type: FWFLine
        assert isinstance(rec1, FWFLine)
        assert rec1.lineno == 0
        assert rec1["birthday"] == b"19570526"

        rec2 = fwf[0:1]
        assert isinstance(rec2, FWFRegion)
        assert slice(rec2.start, rec2.stop) == slice(0, 1)
        assert len(rec2) == 1
        assert len(list(rec2)) == 1
        for r in rec2:
            assert r["birthday"] in [b"19570526", b"19940213"]

        rec1 = fwf[5]
        assert isinstance(rec1, FWFLine)
        assert rec1.lineno == 5
        assert rec1["birthday"] == b"19770319"

        rec1 = fwf[-1]
        assert isinstance(rec1, FWFLine)
        assert rec1.lineno == 9
        assert rec1["birthday"] == b"20080503"

        rec2 = fwf[0:5]
        assert isinstance(rec2, FWFRegion)
        assert slice(rec2.start, rec2.stop) == slice(0, 5)
        assert len(list(rec2)) == len(rec2)

        rec2 = fwf[-5:]
        assert isinstance(rec2, FWFRegion)
        assert slice(rec2.start, rec2.stop) == slice(5, 10)
        assert len(list(rec2)) == len(rec2)

        rec2 = fwf[0:0]
        assert isinstance(rec2, FWFRegion)
        assert slice(rec2.start, rec2.stop) == slice(0, 0)
        assert len(list(rec2)) == len(rec2)


def test_index_selector():
    with fwf_open(HumanFile, DATA) as fwf:
        # Unique on an index view
        rtn = fwf[0, 2, 5]
        assert isinstance(rtn, FWFSubset)
        assert len(list(rtn)) == 3
        assert len(rtn) == 3
        for r in rtn:
            assert r.rooted().lineno in [0, 2, 5]
            assert r["gender"] in [b"M", b"F"]

        rtn2 = fwf[0:6][0, 2, 5]
        assert isinstance(rtn2, FWFSubset)
        assert rtn.lines == rtn2.lines


def test_boolean_selector():
    with fwf_open(HumanFile, DATA) as fwf:
        # Unique on an boolean view
        rtn = fwf[True, False, True, False, False, True]
        assert len(list(rtn)) == 3
        assert len(rtn) == 3
        for r in rtn:
            assert r.rooted().lineno in [0, 2, 5]
            assert r["gender"] in [b"M", b"F"]


def test_multiple_selectors():
    with fwf_open(HumanFile, DATA) as fwf:
        rec = fwf[:]
        assert isinstance(rec, FWFRegion)
        assert slice(rec.start, rec.stop) == slice(0, 10)
        assert len(rec) == 10

        rec = rec[:]
        assert isinstance(rec, FWFRegion)
        assert slice(rec.start, rec.stop) == slice(0, 10)
        assert len(rec) == 10

        x = [1, 2, 3, 4, 5, 6]
        y = x[0:-1]
        assert 6 not in y

        rec = rec[0:-1]
        assert isinstance(rec, FWFRegion)
        assert slice(rec.start, rec.stop) == slice(0, 9)
        assert len(rec) == 9

        rec = rec[2:-2]
        assert isinstance(rec, FWFRegion)
        assert slice(rec.start, rec.stop) == slice(2, 7)
        assert len(rec) == 5

        with pytest.raises(Exception):
            rec = rec[1, 2, 4, 5]

        rec = rec[0, 1, 2, 4]
        assert isinstance(rec, FWFSubset)
        assert rec.lines == [2, 3, 4, 6]
        assert len(rec) == 4

        rec = rec[True, False, True]    # Implicit false, if True/False list is shorter
        assert isinstance(rec, FWFSubset)
        assert rec.lines == [2, 4]
        assert len(rec) == 2

        rec = fwf[:][:][0:-1][2:-2][1, 2, 4][True, False, True]
        assert isinstance(rec, FWFSubset)
        assert rec.lines == [3, 6]
        assert len(rec) == 2


def test_table_filter_by_line():
    with fwf_open(HumanFile, DATA) as fwf:
        rtn = fwf.filter(lambda l: l[19] == ord('F'))
        assert len(list(rtn)) == 7
        assert len(rtn) == 7

        rtn = fwf.filter(lambda l: l[fwf.fields["gender"].start] == ord('M'))
        assert len(list(rtn)) == 3
        assert len(rtn) == 3

        rtn = fwf.filter(lambda l: l[fwf.fields["state"].fslice] == b'AR')
        assert len(list(rtn)) == 2
        assert len(rtn) == 2

        rtn = fwf.filter(lambda l: l[fwf.fields["state"].fslice] == b'XX')
        assert len(list(rtn)) == 0
        assert len(rtn) == 0

        # We often need to filter or sort by a date
        assert b"19000101" < b"20191231"
        assert b"20190101" < b"20190102"
        assert b"20190101" == b"20190101"
        assert b"20190102" > b"20190101"
        assert b"20200102" > b"20190101"


def test_table_filter_by_field():
    with fwf_open(HumanFile, DATA) as fwf:
        rtn = fwf.filter("gender", lambda x: x == b'F')
        assert len(list(rtn)) == 7

        rtn = fwf.filter("gender", b'F')
        assert len(list(rtn)) == 7

        rtn = fwf.filter("gender", lambda x: x == b'M')
        assert len(list(rtn)) == 3

        rtn = fwf.filter("gender", b'M')
        assert len(list(rtn)) == 3

        rtn = fwf.filter("gender", lambda x: x in [b'M', b'F'])
        assert len(list(rtn)) == 10


def test_table_filter_by_function():
    with fwf_open(HumanFile, DATA) as fwf:
        gender = fwf.fields["gender"]
        state = fwf.fields["state"]

        def my_complex_reusable_test(line):
            # Not very efficient, but shows that you can do essentially everything
            rtn = (line[gender] == b'F') and str(line[state], "utf-8").startswith('A')
            return rtn

        rtn = fwf[:].filter(my_complex_reusable_test)
        assert len(list(rtn)) == 2


def test_view_of_a_view():
    with fwf_open(HumanFile, DATA) as fwf:
        rec = fwf[1:8]
        assert fwf.fields == rec.fields
        assert len(list(rec)) == 7
        assert len(rec) == 7

        rtn = rec[2:4]
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        for rec in rtn:
            assert rec.rooted().lineno in [3, 4]

        rtn = fwf.filter_by_field("gender", b'F')
        assert len(list(rtn)) == 7
        assert len(rtn) == 7
        for rec in rtn:
            assert rec.rooted().lineno in [0, 3, 5, 6, 7, 8, 9]

        for rec in rtn[2:4]:
            assert rec.rooted().lineno in [5, 6]


def exec_empty_data(data):
    with fwf_open(HumanFile, data) as fwf:
        assert len(fwf) == 0

        for _ in fwf:
            raise Exception("Should be empty")

        for _ in fwf.iter_lines():
            raise Exception("Should be empty")

        assert len(list(x for x in fwf)) == 0

        with pytest.raises(Exception):
            rtn = fwf[0:-1]

        rtn = fwf.filter_by_field("gender", b'F')
        assert len(rtn) == 0


def test_empty_data():
    with pytest.raises(Exception):
        exec_empty_data(None)

    exec_empty_data(b"")
    exec_empty_data(b"# Empty")
    exec_empty_data(b"# Empty\n")
