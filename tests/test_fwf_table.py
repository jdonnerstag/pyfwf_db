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


def test_open():
    
    # We need both option: 
    # 1) using a with statement and automatically close again
    # 2) manually open and close it
    
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA) as fd:
        assert fd.mm is not None

    assert fd.mm is None
    assert fwf.mm is None

    fd = fwf.open(DATA)
    assert fd is not None
    assert fd is fwf
    assert fd.mm is not None

    fd.close()
    assert fd.mm is None
    assert fwf.mm is None

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

        # Same with full file slice
        for rec in fwf[:]:
            assert rec
            assert rec.line
            assert rec.lineno < 10 # we have 10 rows in our test data

        assert rec.lineno == 9 


def test_table_line_selector():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rec = fwf[0]
        assert rec.lineno == 0
        assert rec["birthday"] == b"19570526"

        rec = fwf[0:1]
        assert rec.lines == slice(0, 1)
        assert len(list(rec)) == 1
        for r in rec:
            assert r["birthday"] in [b"19570526", b"19940213"]

        rec = fwf[5]
        assert rec.lineno == 5
        assert rec["birthday"] == b"19770319"

        rec = fwf[-1]
        assert rec.lineno == 9
        assert rec["birthday"] == b"20080503"

        rec = fwf[0:5]
        assert rec.lines == slice(0, 5)
        assert len(list(rec)) == 5

        rec = fwf[-5:]
        assert rec.lines == slice(5, 10)
        assert len(list(rec)) == 5

        rec = fwf[0:0]
        assert rec.lines == slice(0, 0)
        assert len(list(rec)) == 0


def test_index_selector():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        # Unique on an index view
        rtn = fwf[0, 2, 5]
        assert len(list(rtn)) == 3
        assert len(rtn) == 3
        for r in rtn:
            assert r.lineno in [0, 2, 5]
            assert r["gender"] in [b"M", b"F"]

        rtn2 = fwf[0:6][0, 2, 5]
        assert rtn.lines == rtn2.lines


def test_boolean_selector():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        # Unique on an boolean view
        rtn = fwf[True, False, True, False, False, True]
        assert len(list(rtn)) == 3
        assert len(rtn) == 3
        for r in rtn:
            assert r.lineno in [0, 2, 5]
            assert r["gender"] in [b"M", b"F"]


def test_multiple_selectors():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rec = fwf[:]
        assert rec.lines == slice(0, 10)
        assert len(rec) == 10

        rec = rec[:]
        assert rec.lines == slice(0, 10)
        assert len(rec) == 10

        x = [1, 2, 3, 4, 5, 6]
        y = x[0:-1]
        assert 6 not in y

        rec = rec[0:-1]
        assert rec.lines == slice(0, 9)
        assert len(rec) == 9

        rec = rec[2:-2]
        assert rec.lines == slice(2, 7)
        assert len(rec) == 5

        with pytest.raises(Exception):
            rec = rec[1, 2, 4, 5, 6]

        rec = rec[1, 2, 4, 5]
        assert rec.lines == [3, 4, 6, 7]  # 6 is out of range
        assert len(rec) == 4

        rec = rec[True, False, True]
        assert rec.lines == [3, 6]
        assert len(rec) == 2

        rec = fwf[:][:][0:-1][2:-2][1, 2, 4, 5][True, False, True]
        assert rec.lines == [3, 6]
        assert len(rec) == 2


def test_table_filter_by_line():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = fwf.filter(lambda l: l[19] == ord('F'))
        assert len(list(rtn)) == 7
        assert len(rtn) == 7

        rtn = fwf.filter(lambda l: l[fwf.fields["gender"].start] == ord('M'))
        assert len(list(rtn)) == 3
        assert len(rtn) == 3

        rtn = fwf.filter(lambda l: l[fwf.fields["state"]] == b'AR')
        assert len(list(rtn)) == 2
        assert len(rtn) == 2

        rtn = fwf.filter(lambda l: l[fwf.fields["state"]] == b'XX')
        assert len(list(rtn)) == 0
        assert len(rtn) == 0

        # We often need to filter or sort by a date
        assert b"19000101" < b"20191231"
        assert b"20190101" < b"20190102"
        assert b"20190101" == b"20190101"
        assert b"20190102" > b"20190101"
        assert b"20200102" > b"20190101"


def test_table_filter_by_field():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

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
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        gender = fwf.fields["gender"]
        state = fwf.fields["state"]

        def my_complex_reusable_test(line):
            # Not very efficient, but shows that you can do essentially everything
            rtn = (line[gender] == b'F') and line[state].decode().startswith('A')
            return rtn

        rtn = fwf[:].filter(my_complex_reusable_test)
        assert len(list(rtn)) == 2


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



def exec_empty_data(data):

    fwf = FWFFile(HumanFile)
    with fwf.open(data):
        assert len(fwf) == 0
 
        for rec in fwf:
            raise Exception("Should be empty")

        for rec in fwf.iter_lines():
            raise Exception("Should be empty")

        assert list(x for x in fwf) == []
        
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


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_view_of_a_view()
