#!/usr/bin/env python
# encoding: utf-8

import pytest

import os
import sys
import io

from fwf_db import FWFTable, FWFSimpleIndex, FWFMultiView


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


class HumanFile(object):

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


def test_bytes_input():
    fwf = FWFTable(HumanFile)
    with fwf.open(DATA):
        assert fwf.encoding is None
        assert len(fwf.columns) == 8
        assert fwf.fwidth == 83
        assert fwf.fd == None
        assert fwf.mm == None
        assert fwf.mv != None
        assert fwf.start_pos == 18
        assert fwf.reclen == 10
        assert len(fwf) == 10
        assert fwf.lines


def test_file_input():
    fwf = FWFTable(HumanFile)
    with fwf.open("./examples/humans.txt"):
        assert fwf.encoding is None
        assert len(fwf.columns) == 8
        assert fwf.fwidth == 83
        assert fwf.fd != None
        assert fwf.mm != None
        assert fwf.mv != None
        assert fwf.reclen == 10012
        assert fwf.start_pos == 0
        assert len(fwf) == 10012
        assert fwf.lines


def test_table_iter():
    fwf = FWFTable(HumanFile)
    with fwf.open(DATA):
        for rec in fwf:
            assert rec
            assert rec.line
            assert rec.idx < 10 # we have 10 rows in our test data

        assert rec.idx == 9 


def test_table_line_selector():
    fwf = FWFTable(HumanFile)
    with fwf.open(DATA):

        rec = fwf[0]
        assert fwf.columns == rec.columns
        assert rec.lines == slice(0, 1)
        assert len(list(rec)) == 1

        rec = fwf[5]
        assert rec.lines == slice(5, 6)
        assert len(list(rec)) == 1

        rec = fwf[-1]
        assert rec.lines == slice(9, 10)
        assert len(list(rec)) == 1

        rec = fwf[0:5]
        assert rec.lines == slice(0, 5)
        assert len(list(rec)) == 5

        rec = fwf[-5:]
        assert rec.lines == slice(5, 10)
        assert len(list(rec)) == 5

        rec = fwf[0:0]
        assert rec.lines == slice(0, 0)
        assert len(list(rec)) == 0


def test_table_colume_selector():
    fwf = FWFTable(HumanFile)
    with fwf.open(DATA):

        rec = fwf[0, "TRANCODE"]
        assert len(rec.columns) == 1
        assert rec.lines == slice(0, 1)

        rec = fwf[0, ["TRANCODE", "BUSINESS_DATE"]]
        assert len(rec.columns) == 2
        assert rec.lines == slice(0, 1)

        rec = fwf[0:5, ["TRANCODE", "BUSINESS_DATE"]]
        assert len(rec.columns) == 2
        assert rec.lines == slice(0, 5)


def test_table_filter_by_line():
    fwf = FWFTable(HumanFile)
    with fwf.open(DATA):

        rtn = fwf.filter_by_line(lambda l: l[19] == ord('F'))
        assert len(list(rtn)) == 7
        assert len(rtn) == 7

        rtn = fwf.filter_by_line(lambda l: l[fwf.columns["gender"].start] == ord('M'))
        assert len(list(rtn)) == 3
        assert len(rtn) == 3

        rtn = fwf.filter_by_line(lambda l: l[fwf.columns["state"]] == b'AR')
        assert len(list(rtn)) == 2
        assert len(rtn) == 2

        rtn = fwf.filter_by_line(lambda l: l[fwf.columns["state"]] == b'XX')
        assert len(list(rtn)) == 0
        assert len(rtn) == 0


def test_table_filter_by_field():
    fwf = FWFTable(HumanFile)
    with fwf.open(DATA):

        rtn = fwf.filter_by_field("gender", lambda x: x == b'F')
        assert len(list(rtn)) == 7

        rtn = fwf.filter_by_field("gender", b'F')
        assert len(list(rtn)) == 7

        rtn = fwf.filter_by_field("gender", lambda x: x == b'M')
        assert len(list(rtn)) == 3

        rtn = fwf.filter_by_field("gender", b'M')
        assert len(list(rtn)) == 3

        rtn = fwf.filter_by_field("gender", lambda x: x in [b'M', b'F'])
        assert len(list(rtn)) == 10


def test_table_filter_by_function():
    fwf = FWFTable(HumanFile)
    with fwf.open(DATA):

        gender = fwf.columns["gender"]
        state = fwf.columns["state"]

        def my_complex_reusable_test(line):
            # Not very efficient, but shows that you can do essentially everything
            rtn = (line[gender] == b'F') and line[state].tobytes().decode().startswith('A')
            return rtn

        rtn = fwf[:].filter_by_line(my_complex_reusable_test)
        assert len(list(rtn)) == 2


def test_unique():
    fwf = FWFTable(HumanFile)
    with fwf.open(DATA):

        rtn = fwf.unique("state")
        assert len(list(rtn)) == 9
        assert len(rtn) == 9

        rtn = fwf.unique("state", lambda x: x.decode())
        assert len(list(rtn)) == 9
        assert len(rtn) == 9
        assert "MI" in rtn

        def to_str(x):
            return x.decode()

        rtn = fwf.unique("gender", to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn

        # Unique on a view
        x = fwf[0:5]
        rtn = x.unique("gender", to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn


def test_index():
    fwf = FWFTable(HumanFile)
    with fwf.open(DATA):

        rtn = FWFSimpleIndex(fwf).index("state")
        assert len(list(rtn.idx)) == 9
        assert len(rtn) == 9

        rtn = FWFSimpleIndex(fwf).index("gender")
        assert len(list(rtn.idx)) == 2
        assert len(rtn) == 2

        rtn = FWFSimpleIndex(fwf).index("state", lambda x: x.tobytes().decode())
        assert len(list(rtn.idx)) == 9
        assert rtn.loc("MI")
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFSimpleIndex(fwf).index("gender", lambda x: x.tobytes().decode())
        assert len(list(rtn.idx)) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert rtn["M"]
        assert rtn["F"]
        assert not rtn["xxx"]

        for rec in rtn["M"]:
            assert rec.idx in [1, 2, 4]

        rtn = FWFSimpleIndex(fwf).index(1)  # Also works with integers == state
        assert len(list(rtn.idx)) == 9
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        rtn = FWFSimpleIndex(x).index("state")
        assert len(list(rtn.idx)) == 5
        assert len(rtn) == 5


def test_view_of_a_view():
    fwf = FWFTable(HumanFile)
    with fwf.open(DATA):

        rec = fwf[1:8]
        assert fwf.columns == rec.columns
        assert len(list(rec)) == 7
        assert len(rec) == 7

        rtn = rec[2:4]
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        for rec in rtn:       # pylint: this is a false-positive, as the code clearly works well
            assert rec.idx in [3, 4]

        rtn = fwf.filter_by_field("gender", b'F')
        assert len(list(rtn)) == 7
        assert len(rtn) == 7
        for rec in rtn:
            assert rec.idx in [0, 3, 5, 6, 7, 8, 9]

        for rec in rtn[2:4]:  # pylint: this is a false-positive, as the code clearly works well
            assert rec.idx in [5, 6]


def test_multi_view():
    fwf1 = FWFTable(HumanFile)
    with fwf1.open(DATA):
        fwf2 = FWFTable(HumanFile)
        with fwf2.open(DATA):

            mf = FWFMultiView()
            mf.add_file(fwf1)
            mf.add_file(fwf2)

            assert len(mf) == 20
            assert len(mf.files) == 2
            assert mf.lines == slice(0, 20)

            assert len(list(mf)) == 20

            for idx, _ in mf.iter_lines():
                assert idx < 20

            for rec in mf:
                assert rec.idx < 20

            x = mf[0]
            assert len(x) == 1
            assert len(mf[5]) == 1
            assert len(mf[10]) == 1
            assert len(mf[15]) == 1
            assert len(mf[19]) == 1
            assert len(mf[0:5]) == 5
            assert len(mf[-5:]) == 5
            assert len(mf[5:15]) == 10
            pass

"""

        rtn = fwf[0:5].to_pandas()

 """


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_table_iter()
