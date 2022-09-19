#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name, missing-module-docstring

import pytest

import numpy as np

from fwf_db.fwf_file import FWFFile
from fwf_db.fwf_subset import FWFSubset
from fwf_db.fwf_line import FWFLine
from fwf_db.fwf_index_like import FWFIndexDict, FWFUniqueIndexDict
from fwf_db.fwf_simple_index import FWFSimpleIndexBuilder
from fwf_db.fwf_np_index import FWFNumpyIndexBuilder
from fwf_db.fwf_cython_index import FWFCythonIndexBuilder
from fwf_db._cython.fwf_mem_optimized_index import BytesDictWithIntListValues


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

        rtn = FWFIndexDict(fwf)
        FWFSimpleIndexBuilder(rtn).index(fwf, "state")
        assert len(rtn) == 9

        rtn = FWFIndexDict(fwf)
        FWFSimpleIndexBuilder(rtn).index(fwf, "gender")
        assert len(rtn) == 2

        rtn = FWFIndexDict(fwf)
        FWFSimpleIndexBuilder(rtn).index(fwf, "state", func=lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFIndexDict(fwf)
        FWFSimpleIndexBuilder(rtn).index(fwf, "gender", func=lambda x: x.decode())
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

        rtn = FWFIndexDict(fwf)
        FWFSimpleIndexBuilder(rtn).index(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        rtn = FWFIndexDict(fwf)
        FWFSimpleIndexBuilder(rtn).index(x, "state")
        assert len(rtn) == 5


def test_simple_index_with_mem_optimized_dict():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        data = BytesDictWithIntListValues(len(fwf))
        rtn = FWFIndexDict(fwf, data)
        FWFSimpleIndexBuilder(rtn).index(fwf, "state")
        assert len(rtn) == 9
        data.finish()
        assert len(rtn) == 9

        data = BytesDictWithIntListValues(len(fwf))
        rtn = FWFIndexDict(fwf, data)
        FWFSimpleIndexBuilder(rtn).index(fwf, "gender")
        assert len(rtn) == 2
        data.finish()
        assert len(rtn) == 2

        data = BytesDictWithIntListValues(len(fwf))
        rtn = FWFIndexDict(fwf, data)
        FWFSimpleIndexBuilder(rtn).index(fwf, "state", func=lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]
        data.finish()
        assert "MI" in rtn
        assert rtn["MI"]

        data = BytesDictWithIntListValues(len(fwf))
        rtn = FWFIndexDict(fwf, data)
        FWFSimpleIndexBuilder(rtn).index(fwf, "gender", func=lambda x: x.decode())
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn
        assert "xxx" not in rtn
        assert rtn["M"]
        assert rtn["F"]
        data.finish()
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
            np.testing.assert_array_equal(rec.lines, value.lines)
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

        data = BytesDictWithIntListValues(len(fwf))
        rtn = FWFIndexDict(fwf, data)
        FWFSimpleIndexBuilder(rtn).index(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9
        data.finish()
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        data = BytesDictWithIntListValues(len(x))
        rtn = FWFIndexDict(x, data)
        FWFSimpleIndexBuilder(rtn).index(x, "state")
        assert len(rtn) == 5
        data.finish()
        assert len(rtn) == 5


def test_np_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFIndexDict(fwf)
        FWFNumpyIndexBuilder(rtn).index(fwf, "state", dtype=(np.bytes_, 2))
        assert len(rtn) == 9

        rtn = FWFIndexDict(fwf)
        FWFNumpyIndexBuilder(rtn).index(fwf, "gender", dtype=(np.bytes_, 1))
        assert len(rtn) == 2

        rtn = FWFIndexDict(fwf)
        FWFNumpyIndexBuilder(rtn).index(fwf, "state", dtype="U2", func=lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFIndexDict(fwf)
        FWFNumpyIndexBuilder(rtn).index(fwf, "gender", dtype="U1", func=lambda x: x.decode())
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

        rtn = FWFIndexDict(fwf)
        FWFNumpyIndexBuilder(rtn).index(fwf, 1, dtype=(np.bytes_, 8))  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        rtn = FWFIndexDict(fwf)
        FWFNumpyIndexBuilder(rtn).index(x, "state", dtype=(np.bytes_, 2))
        assert len(rtn) == 5


# TODO If the tests for the different index implementations are the same, can we re-use them?
def test_cython_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFIndexDict(fwf)
        FWFCythonIndexBuilder(rtn).index(fwf, "state")
        assert len(rtn) == 9

        rtn = FWFIndexDict(fwf)
        FWFCythonIndexBuilder(rtn).index(fwf, "gender")
        assert len(rtn) == 2

        rtn = FWFIndexDict(fwf)
        FWFCythonIndexBuilder(rtn).index(fwf, "state", func=lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFIndexDict(fwf)
        FWFCythonIndexBuilder(rtn).index(fwf, "gender", func=lambda x: x.decode())
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

        rtn = FWFIndexDict(fwf)
        FWFCythonIndexBuilder(rtn).index(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        # Cython index is only available on FWFile. It wouldn't be faster then
        # an ordinary Index.
        x = fwf[0:5]
        rtn = FWFIndexDict(x)
        with pytest.raises(TypeError):
            FWFCythonIndexBuilder(rtn).index(x, "state")      # type: ignore


def test_simple_unique_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFSimpleIndexBuilder(rtn).index(fwf, "state")
        assert len(rtn) == 9

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFSimpleIndexBuilder(rtn).index(fwf, "gender")
        assert len(rtn) == 2

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFSimpleIndexBuilder(rtn).index(fwf, "state", func=lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFSimpleIndexBuilder(rtn).index(fwf, "gender", func=lambda x: x.decode())
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

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFSimpleIndexBuilder(rtn).index(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        x = fwf[0:5]
        rtn = FWFUniqueIndexDict(fwf, {})
        FWFSimpleIndexBuilder(rtn).index(x, "state")
        assert len(rtn) == 5


def test_cython_unique_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFCythonIndexBuilder(rtn).index(fwf, "state")
        assert len(rtn) == 9

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFCythonIndexBuilder(rtn).index(fwf, "gender")
        assert len(rtn) == 2

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFCythonIndexBuilder(rtn).index(fwf, "state", lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFCythonIndexBuilder(rtn).index(fwf, "gender", lambda x: x.decode())
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

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFCythonIndexBuilder(rtn).index(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        # Cython index is only available on FWFile. It wouldn't be faster then
        # an ordinary Index.
        x = fwf[0:5]
        rtn = FWFUniqueIndexDict(x, {})
        with pytest.raises(TypeError):
            FWFCythonIndexBuilder(rtn).index(x, "state")      # type: ignore


def test_np_unique_index():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFNumpyIndexBuilder(rtn).index(fwf, "state")
        assert len(rtn) == 9

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFNumpyIndexBuilder(rtn).index(fwf, "gender")
        assert len(rtn) == 2

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFNumpyIndexBuilder(rtn).index(fwf, "state", dtype="U2", func=lambda x: x.decode())
        assert "MI" in rtn
        assert rtn["MI"]

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFNumpyIndexBuilder(rtn).index(fwf, "gender", dtype="U1", func=lambda x: x.decode())
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

        rtn = FWFUniqueIndexDict(fwf, {})
        FWFNumpyIndexBuilder(rtn).index(fwf, 1)  # Also works with integers == state
        assert len(rtn) == 9

        # Index on a view
        # Cython index is only available on FWFile. It wouldn't be faster then
        # an ordinary Index.
        x = fwf[0:5]
        rtn = FWFUniqueIndexDict(fwf, {})
        FWFNumpyIndexBuilder(rtn).index(x, "state")

# TODO Add tests that validate that the indexes also work correctly with views (instead of FWFile)
