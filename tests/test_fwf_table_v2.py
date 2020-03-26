#!/usr/bin/env python
# encoding: utf-8

import pytest

import os
import sys
import io

from fwf_dbv2 import FWFFile, FWFSimpleIndex, FWFMultiFile, FWFUnique


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
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):
        assert fwf.encoding is None
        assert len(fwf.fields) == 8
        assert fwf.fwidth == 83
        assert fwf.fd is None
        assert fwf.mm is not None
        assert fwf.start_pos == 18
        assert fwf.reclen == 10
        assert len(fwf) == 10
        assert fwf.lines


def test_file_input():
    fwf = FWFFile(HumanFile)
    with fwf.open("./examples/humans.txt"):
        assert fwf.encoding is None
        assert len(fwf.fields) == 8
        assert fwf.fwidth == 83
        assert fwf.fd is not None
        assert fwf.mm is not None
        assert fwf.reclen == 10012
        assert fwf.start_pos == 0
        assert len(fwf) == 10012
        assert fwf.lines


def test_table_iter():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):
        for rec in fwf:
            assert rec
            assert rec.line
            assert rec.lineno < 10 # we have 10 rows in our test data

        assert rec.lineno == 9 


def test_table_line_selector():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rec = fwf[0]
        assert rec.lineno == 0

        rec = fwf[0:1]
        assert rec.lines == slice(0, 1)
        assert len(list(rec)) == 1

        rec = fwf[5]
        assert rec.lineno == 5

        rec = fwf[-1]
        assert rec.lineno == 9

        rec = fwf[0:5]
        assert rec.lines == slice(0, 5)
        assert len(list(rec)) == 5

        rec = fwf[-5:]
        assert rec.lines == slice(5, 10)
        assert len(list(rec)) == 5

        rec = fwf[0:0]
        assert rec.lines == slice(0, 0)
        assert len(list(rec)) == 0


def test_table_filter_by_line():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = fwf.filter_by_line(lambda l: l[19] == ord('F'))
        assert len(list(rtn)) == 7
        assert len(rtn) == 7

        rtn = fwf.filter_by_line(lambda l: l[fwf.fields["gender"].start] == ord('M'))
        assert len(list(rtn)) == 3
        assert len(rtn) == 3

        rtn = fwf.filter_by_line(lambda l: l[fwf.fields["state"]] == b'AR')
        assert len(list(rtn)) == 2
        assert len(rtn) == 2

        rtn = fwf.filter_by_line(lambda l: l[fwf.fields["state"]] == b'XX')
        assert len(list(rtn)) == 0
        assert len(rtn) == 0


def test_table_filter_by_field():
    fwf = FWFFile(HumanFile)
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
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        gender = fwf.fields["gender"]
        state = fwf.fields["state"]

        def my_complex_reusable_test(line):
            # Not very efficient, but shows that you can do essentially everything
            rtn = (line[gender] == b'F') and line[state].decode().startswith('A')
            return rtn

        rtn = fwf[:].filter_by_line(my_complex_reusable_test)
        assert len(list(rtn)) == 2


def test_unique():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFUnique(fwf).unique("state")
        assert len(list(rtn)) == 9
        assert len(rtn) == 9

        rtn = FWFUnique(fwf).unique("state", lambda x: x.decode())
        assert len(list(rtn)) == 9
        assert len(rtn) == 9
        assert "MI" in rtn

        def to_str(x):
            return x.decode()

        rtn = FWFUnique(fwf).unique("gender", to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn

        # Unique on a view
        x = fwf[0:5]
        rtn = FWFUnique(x).unique("gender", to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn


def test_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFSimpleIndex(fwf).index("state")
        assert len(rtn) == 9

        rtn = FWFSimpleIndex(fwf).index("gender")
        assert len(rtn) == 2

        rtn = FWFSimpleIndex(fwf).index("state", lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFSimpleIndex(fwf).index("gender", lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert rtn["M"]
        assert rtn["F"]
        assert not rtn["xxx"]

        for rec in rtn["M"]:
            assert rec.lineno in [1, 2, 4]

        x = rtn["M"]
        x = x[2]
        assert rtn["M"][2].lineno == 4

        rtn = FWFSimpleIndex(fwf).index(1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        rtn = FWFSimpleIndex(x).index("state")
        assert len(rtn) == 5


def test_view_of_a_view():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rec = fwf[1:8]
        assert fwf.fields == rec.fields
        assert len(list(rec)) == 7
        assert len(rec) == 7

        rtn = rec[2:4]
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        for rec in rtn:       # pylint: this is a false-positive, as the code clearly works well
            assert rec.lineno in [3, 4]

        rtn = fwf.filter_by_field("gender", b'F')
        assert len(list(rtn)) == 7
        assert len(rtn) == 7
        for rec in rtn:
            assert rec.lineno in [0, 3, 5, 6, 7, 8, 9]

        for rec in rtn[2:4]:  # pylint: this is a false-positive, as the code clearly works well
            assert rec.lineno in [5, 6]


def test_multi_view():
    fwf1 = FWFFile(HumanFile)
    with fwf1.open(DATA):
        fwf2 = FWFFile(HumanFile)
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

"""

        rtn = fwf[0:5].to_pandas()

 """


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_table_iter()