#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name, missing-module-docstring

import pytest

import numpy as np

from fwf_db.fwf_file import FWFFile
from fwf_db.fwf_simple_index import FWFSimpleIndex
from fwf_db.fwf_np_unique_index import FWFUniqueNumpyIndex
from fwf_db.fwf_np_index import FWFNumpyIndex
from fwf_db.fwf_cython_index import FWFCythonIndex
from fwf_db.fwf_cython_unique_index import FWFCythonUniqueIndex
from fwf_db.fwf_simple_unique_index import FWFSimpleUniqueIndex
from fwf_db.fwf_subset import FWFSubset
from fwf_db.fwf_line import FWFLine


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


def test_simple_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFSimpleIndex(fwf, "state")
        assert len(rtn) == 9

        rtn = FWFSimpleIndex(fwf, "gender")
        assert len(rtn) == 2

        rtn = FWFSimpleIndex(fwf, "state", lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFSimpleIndex(fwf, "gender", lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert "xxx" not in rtn
        assert rtn["M"]
        assert rtn["F"]

        with pytest.raises(KeyError):
            _ = rtn["xxx"]

        for key, value in rtn:
            assert key in ["F", "M"]
            rec = rtn[key]
            assert rec.lines == value.lines
            assert len(rec) == 3 or len(rec) == 7

        x = FWFSubset(rtn.fwfview, rtn.data["M"], rtn.fwfview.fields)
        assert isinstance(x, FWFSubset)
        for rec in x:
            assert rec.rooted().lineno in [1, 2, 4]

        x = rtn.get("M")
        assert isinstance(x, FWFSubset)
        for rec in x:
            assert rec.rooted().lineno in [1, 2, 4]

        x = rtn["M"]
        assert isinstance(x, FWFSubset)
        for rec in x:
            assert rec.rooted().lineno in [1, 2, 4]

        x = rtn["M"]
        assert isinstance(x, FWFSubset)
        x = x[2]
        assert isinstance(x, FWFLine)
        assert rtn["M"][2].rooted().lineno == 4

        rtn = FWFSimpleIndex(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        rtn = FWFSimpleIndex(x, "state")
        assert len(rtn) == 5


def test_np_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFNumpyIndex(fwf, "state", dtype=(np.bytes_, 2))
        assert len(rtn) == 9

        rtn = FWFNumpyIndex(fwf, "gender", dtype=(np.bytes_, 1))
        assert len(rtn) == 2

        rtn = FWFNumpyIndex(fwf, "state", dtype="U2", func=lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFNumpyIndex(fwf, "gender", dtype="U1", func=lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert "xxx" not in rtn
        assert rtn["M"]
        assert rtn["F"]

        with pytest.raises(KeyError):
            _ = rtn["xxx"]

        for key, _ in rtn:
            assert key in ["F", "M"]
            rec = rtn[key]
            assert len(rec) == 3 or len(rec) == 7

        for rec in rtn["M"]:
            assert rec.rooted().lineno in [1, 2, 4]

        x = rtn["M"]
        x = x[2]
        assert rtn["M"][2].rooted().lineno == 4

        rtn = FWFNumpyIndex(fwf, 1, dtype=(np.bytes_, 8))  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        rtn = FWFNumpyIndex(x, "state", dtype=(np.bytes_, 2))
        assert len(rtn) == 5


# TODO If the tests for the different index implementations are the same, can we re-use them?
def test_cython_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFCythonIndex(fwf, "state")
        assert len(rtn) == 9

        rtn = FWFCythonIndex(fwf, "gender")
        assert len(rtn) == 2

        rtn = FWFCythonIndex(fwf, "state", lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFCythonIndex(fwf, "gender", lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert "xxx" not in rtn
        assert rtn["M"]
        assert rtn["F"]

        with pytest.raises(KeyError):
            _ = rtn["xxx"]

        for key, _ in rtn:
            assert key in ["F", "M"]
            rec = rtn[key]
            assert len(rec) == 3 or len(rec) == 7

        for rec in rtn["M"]:
            assert rec.rooted().lineno in [1, 2, 4]

        x = rtn["M"]
        x = x[2]
        assert rtn["M"][2].rooted().lineno == 4

        rtn = FWFCythonIndex(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        # Cython index is only available on FWFile. It wouldn't be faster then
        # an ordinary Index.
        x = fwf[0:5]
        with pytest.raises(AssertionError):
            rtn = FWFCythonIndex(x, "state")      # type: ignore


def test_simple_unique_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFSimpleUniqueIndex(fwf, "state")
        assert len(rtn) == 9

        rtn = FWFSimpleUniqueIndex(fwf, "gender")
        assert len(rtn) == 2

        rtn = FWFSimpleUniqueIndex(fwf, "state", lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFSimpleUniqueIndex(fwf, "gender", lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert "xxx" not in rtn
        assert rtn["M"]
        assert rtn["F"]

        with pytest.raises(KeyError):
            _ = rtn["xxx"]

        # A Unique index doesn't return a list but the single record
        # for rec in rtn["M"]:
        #    assert rec.lineno in [1, 2, 4]

        # x = rtn["M"]
        # x = x[2]
        # assert rtn["M"][2].lineno == 4
        assert rtn["M"].lineno == 4

        rtn = FWFSimpleUniqueIndex(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        rtn = FWFSimpleUniqueIndex(x, "state")
        assert len(rtn) == 5


def test_cython_unique_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFCythonUniqueIndex(fwf, "state")
        assert len(rtn) == 9

        rtn = FWFCythonUniqueIndex(fwf, "gender")
        assert len(rtn) == 2

        rtn = FWFCythonUniqueIndex(fwf, "state", lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFCythonUniqueIndex(fwf, "gender", lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert "xxx" not in rtn
        assert rtn["M"]
        assert rtn["F"]

        with pytest.raises(KeyError):
            _ = rtn["xxx"]

        # A Unique index doesn't return a list but the single record
        # for rec in rtn["M"]:
        #    assert rec.lineno in [1, 2, 4]

        # x = rtn["M"]
        # x = x[2]
        # assert rtn["M"][2].lineno == 4
        assert rtn["M"].lineno == 4

        rtn = FWFCythonUniqueIndex(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        # Cython index is only available on FWFile. It wouldn't be faster then
        # an ordinary Index.
        x = fwf[0:5]
        with pytest.raises(AssertionError):
            rtn = FWFCythonUniqueIndex(x, "state")      # type: ignore


def test_np_unique_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFUniqueNumpyIndex(fwf, "state")
        assert len(rtn) == 9

        rtn = FWFUniqueNumpyIndex(fwf, "gender")
        assert len(rtn) == 2

        rtn = FWFUniqueNumpyIndex(fwf, "state", dtype="U2", func=lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFUniqueNumpyIndex(fwf, "gender", dtype="U1", func=lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert "xxx" not in rtn
        assert rtn["M"]
        assert rtn["F"]

        with pytest.raises(KeyError):
            _ = rtn["xxx"]

        # A Unique index doesn't return a list but the single record
        # for rec in rtn["M"]:
        #    assert rec.lineno in [1, 2, 4]

        # x = rtn["M"]
        # x = x[2]
        # assert rtn["M"][2].lineno == 4
        assert rtn["M"].lineno == 4

        rtn = FWFUniqueNumpyIndex(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        # Cython index is only available on FWFile. It wouldn't be faster then
        # an ordinary Index.
        x = fwf[0:5]
        rtn = FWFUniqueNumpyIndex(x, "state")

# TODO Add tests that validate that the indexes also work correctly with views (instead of FWFile)
