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


def test_unique():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFUnique(fwf).unique("state")
        assert len(list(rtn)) == 9
        assert len(rtn) == 9

        # Transform the value before adding them to unique 
        rtn = FWFUnique(fwf).unique("state", lambda x: x.decode())
        assert len(list(rtn)) == 9
        assert len(rtn) == 9
        assert "MI" in rtn

        # If the func accepts a single value, it can be used without lambda
        def to_str(x):
            return x.decode()

        rtn = FWFUnique(fwf).unique("gender", to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn

        # Unique on a region view
        rtn = FWFUnique(fwf[0:5]).unique("gender", to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn

        # Unique on an index view
        rtn = FWFUnique(fwf[0, 2, 5]).unique("gender", to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn

        # Unique on an boolean view
        rtn = FWFUnique(fwf[True, False, True, False, False, True]).unique("gender", to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn


def test_unique_numpy():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFUniqueNpBased(fwf, (np.bytes_, 2)).unique("state")
        assert len(list(rtn)) == 9
        assert len(rtn) == 9

        rtn = FWFUniqueNpBased(fwf, "U2").unique("state", lambda x: x.decode())
        assert len(list(rtn)) == 9
        assert len(rtn) == 9
        assert "MI" in rtn

        def to_str(x):
            return x.decode()

        rtn = FWFUniqueNpBased(fwf, "U1").unique("gender", to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn

        # Unique on a view
        x = fwf[0:5]
        rtn = FWFUniqueNpBased(x, "U1").unique("gender", to_str)
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


def test_index_numpy_based():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFIndexNumpyBased(fwf).index("state", dtype=(np.bytes_, 2))
        assert len(rtn) == 9

        rtn = FWFIndexNumpyBased(fwf).index("gender", dtype=(np.bytes_, 1))
        assert len(rtn) == 2

        rtn = FWFIndexNumpyBased(fwf).index("state", dtype="U2", func=lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFIndexNumpyBased(fwf).index("gender", dtype="U1", func=lambda x: x.decode())
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

        rtn = FWFIndexNumpyBased(fwf).index(1, dtype=(np.bytes_, 8))  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        rtn = FWFIndexNumpyBased(x).index("state", dtype=(np.bytes_, 2))
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

    test_unique_numpy()
