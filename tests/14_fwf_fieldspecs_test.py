#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name, missing-module-docstring

# Current version of pylint not yet working well with python type hints and is causing plenty false positiv.
# pylint: disable=not-an-iterable, unsubscriptable-object

from typing import Any

import pytest

from fwf_db import FWFFileFieldSpecs, FWFFieldSpec, FileFieldSpecs

def test_single():

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id")     # requires either len, start, stop, slice

    field = FWFFieldSpec(startpos=0, name="id", len=2)
    assert field.name == "id"
    assert field.len == 2
    assert field.start == 0
    assert field.stop == 2
    assert field.slice == slice(0, 2)

    field = FWFFieldSpec(startpos=0, name="id", slice=slice(0, 2))
    assert field.name == "id"
    assert field.len == 2
    assert field.start == 0
    assert field.stop == 2
    assert field.slice == slice(0, 2)

    field = FWFFieldSpec(startpos=0, name="id", slice=(0, 2))
    assert field.name == "id"
    assert field.len == 2
    assert field.start == 0
    assert field.stop == 2
    assert field.slice == slice(0, 2)

    field = FWFFieldSpec(startpos=0, name="id", slice=[0, 2])
    assert field.name == "id"
    assert field.len == 2
    assert field.start == 0
    assert field.stop == 2
    assert field.slice == slice(0, 2)

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id", slice=[1, 2, 3])

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id", slice=(1, 2, 3))

    field = FWFFieldSpec(startpos=0, name="id", start=0, stop=2)
    assert field.name == "id"
    assert field.len == 2
    assert field.start == 0
    assert field.stop == 2
    assert field.slice == slice(0, 2)

    field = FWFFieldSpec(startpos=0, name="id", start=0, len=2)
    assert field.name == "id"
    assert field.len == 2
    assert field.start == 0
    assert field.stop == 2
    assert field.slice == slice(0, 2)

    field = FWFFieldSpec(startpos=0, name="id", stop=2, len=2)
    assert field.name == "id"
    assert field.len == 2
    assert field.start == 0
    assert field.stop == 2
    assert field.slice == slice(0, 2)

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id", slice=(1, 2), start=2)

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id", slice=(1, 2), stop=2)

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id", slice=(1, 2), len=2)

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id", start=2, len=2, stop=2)

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id", slice=(0, -1))     # negative len

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id", slice=(-10, 0))    # must be > 0

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id", slice=(0, 2_000))   # too long

    with pytest.raises(Exception):
        _ = FWFFieldSpec(startpos=0, name="id", start=0, stop="a")   # must be int


def test_filespec_ok():
    spec = FWFFileFieldSpecs([
        {"name": "aa", "len": 9},
        {"name": "bb", "slice": slice(10, 12)},
        {"name": "cc", "slice": (10, 12)},
        {"name": "dd", "slice": [10, 12]},
        {"name": "ee", "start": 20, "len": 8},
        {"name": "ff", "stop": 40, "len": 8},
        {"name": "gg", "start": 40, "stop": 50, "dtype": "111", "regex": "222"},
    ])

    aa = spec.get("aa")
    assert aa is not None
    assert aa.name == "aa"
    assert spec["bb"].name == "bb"
    assert spec.get("AA") is None
    assert spec["gg"].dtype == "111"

    assert "dd" in spec.keys()
    assert list(spec.values())[1].name == "bb"

    assert len(spec) == 7
    assert list(spec.names()) == ["aa", "bb", "cc", "dd", "ee", "ff", "gg"]


def test_duplicate_names():
    with pytest.raises(Exception):
        _ = FWFFileFieldSpecs([
            {"name": "aa", "len": 10},
            {"name": "aa", "len": 10},
        ])


def test_invalid_slice():
    with pytest.raises(Exception):
        _ = FWFFileFieldSpecs([
            {"name": "aa", "len": 10},
            {"name": "bb", "slice": (1, 2, 3)},
        ])


def test_apply_defaults():
    spec = FWFFileFieldSpecs([
        {"name": "aa", "len": 9},
        {"name": "bb", "slice": slice(10, 12)},
        {"name": "cc", "slice": (10, 12)},
    ])

    spec.apply_defaults(None)
    for x in spec.values():
        #print(x)
        assert len(x) == 5  # name, len, slice, start, stop

    spec.apply_defaults({})
    for x in spec.values():
        assert len(x) == 5

    spec.apply_defaults(dict(dtype=1))
    assert spec["aa"].dtype == 1
    for x in spec.values():
        assert len(x) == 6
        assert "dtype" in x

    spec["aa"]["regex"] = "test"
    assert "regex" in spec["aa"]


class XYZFileFieldSpecs(FileFieldSpecs[dict]):
    """A file specification for fwf file"""


def test_default_filespec():
    spec = XYZFileFieldSpecs(dict, [
        {"name": "aa", "len": 9},
    ])

    spec = FileFieldSpecs(dict, [
        {"name": "aa", "len": 9},
    ])
