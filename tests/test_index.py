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
from fwf_db.fwf_cython_index import FWFCythonIndex
from fwf_db.fwf_cython_unique_index import FWFCythonUniqueIndex
from fwf_db.fwf_simple_unique_index import FWFSimpleUniqueIndex
from fwf_db.fwf_cython import FWFCython


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


def test_simple_index():
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

        for key in rtn:
            assert key in ["F", "M"]
            rec = rtn[key]
            assert len(rec) == 3 or len(rec) == 7

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

        for key in rtn:
            assert key in ["F", "M"]
            rec = rtn[key]
            assert len(rec) == 3 or len(rec) == 7

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


def test_index_cython():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFCythonIndex(fwf).index("state")
        assert len(rtn) == 9

        rtn = FWFCythonIndex(fwf).index("gender")
        assert len(rtn) == 2

        rtn = FWFCythonIndex(fwf).index("state", lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFCythonIndex(fwf).index("gender", lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert rtn["M"]
        assert rtn["F"]
        assert not rtn["xxx"]

        for key in rtn:
            assert key in ["F", "M"]
            rec = rtn[key]
            assert len(rec) == 3 or len(rec) == 7

        for rec in rtn["M"]:
            assert rec.lineno in [1, 2, 4]

        x = rtn["M"]
        x = x[2]
        assert rtn["M"][2].lineno == 4

        rtn = FWFCythonIndex(fwf).index(1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        # Cython index is only available on FWFile. It wouldn't be faster then
        # an ordinary Index.
        x = fwf[0:5]
        with pytest.raises(Exception):
            rtn = FWFCythonIndex(x).index("state")


def test_simple_unique_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFSimpleUniqueIndex(fwf).index("state")
        assert len(rtn) == 9

        rtn = FWFSimpleUniqueIndex(fwf).index("gender")
        assert len(rtn) == 2

        rtn = FWFSimpleUniqueIndex(fwf).index("state", lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFSimpleUniqueIndex(fwf).index("gender", lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert rtn["M"]
        assert rtn["F"]
        assert not rtn["xxx"]

        # A Unique index doesn't return a list but the single record
        # for rec in rtn["M"]:
        #    assert rec.lineno in [1, 2, 4]

        # x = rtn["M"]
        # x = x[2]
        # assert rtn["M"][2].lineno == 4
        assert rtn["M"].lineno == 4

        rtn = FWFSimpleUniqueIndex(fwf).index(1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        rtn = FWFSimpleUniqueIndex(x).index("state")
        assert len(rtn) == 5


def test_unique_index_cython():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFCythonUniqueIndex(fwf).index("state")
        assert len(rtn) == 9

        rtn = FWFCythonUniqueIndex(fwf).index("gender")
        assert len(rtn) == 2

        rtn = FWFCythonUniqueIndex(fwf).index("state", lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFCythonUniqueIndex(fwf).index("gender", lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert rtn["M"]
        assert rtn["F"]
        assert not rtn["xxx"]

        # A Unique index doesn't return a list but the single record
        # for rec in rtn["M"]:
        #    assert rec.lineno in [1, 2, 4]

        # x = rtn["M"]
        # x = x[2]
        # assert rtn["M"][2].lineno == 4
        assert rtn["M"].lineno == 4

        rtn = FWFCythonUniqueIndex(fwf).index(1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        # Cython index is only available on FWFile. It wouldn't be faster then
        # an ordinary Index.
        x = fwf[0:5]
        with pytest.raises(Exception):
            rtn = FWFCythonUniqueIndex(x).index("state")


def test_fwf_cython_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFCython(fwf).apply(index="state")
        assert len(rtn) == 9

        rtn = FWFCython(fwf).apply(index="gender", func=lambda x: x.decode())
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

        rtn = FWFCython(fwf).apply(index=1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        # Cython index is only available on FWFile. It wouldn't be faster then
        # an ordinary Index.
        x = fwf[0:5]
        with pytest.raises(Exception):
            rtn = FWFCython(x).apply(index="state")


def test_fwf_cython_unique_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFCython(fwf).apply(index="state", unique_index=True)
        assert len(rtn) == 9

        rtn = FWFCython(fwf).apply(index="gender", func=lambda x: x.decode(), unique_index=True)
        assert len(rtn) == 2

        assert "M" in rtn
        assert "F" in rtn
        assert rtn["M"]
        assert rtn["F"]
        assert not rtn["xxx"]

        # rtn["M"] return a FWFLine and iterating over a FWFLine 
        # yields one byte after another.
        # for rec in rtn["M"]:
        #    assert rec.lineno in [1, 2, 4]

        # x = rtn["M"]
        # x = x[2]
        # assert rtn["M"][2].lineno == 4
        assert rtn["M"].lineno == 4

        rtn = FWFCython(fwf).apply(index=1, unique_index=True)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        # Cython index is only available on FWFile. It wouldn't be faster then
        # an ordinary Index.
        x = fwf[0:5]
        with pytest.raises(Exception):
            rtn = FWFCython(x).apply(index="state", unique_index=True)


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_index_numpy_based()
