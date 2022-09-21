#!/usr/bin/env python
# encoding: utf-8

'''
Test FWFLine class
'''

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name

import pytest

from fwf_db import FWFFile, FWFLine


DATA = memoryview(b"""US       AR19570526Fbe56008be36eDianne Mcintosh         Whatever    Medic        #
""")

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


def test_constructor():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)
    assert line


def test_contains():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)
    assert "location" in line
    assert "state" in line
    assert "birthday" in line
    assert "gender" in line
    assert "name" in line
    assert "universe" in line
    assert "profession" in line
    assert "dummy" in line
    assert "xxx" not in line


def test_keys():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)
    names = [x["name"] for x in HumanFile.FIELDSPECS]
    keys = list(line.keys())
    assert keys == names

    for key in line.keys():
        assert key in names


def test_items():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)
    names = [x["name"] for x in HumanFile.FIELDSPECS]

    for k, v in line.items():
        assert k in names
        assert len(v) == (fwf.fields[k].stop - fwf.fields[k].start)
        if k == "state":
            assert v == b"AR"


def test_to_dict():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)
    d = line.to_dict()

    assert len(d) == 8
    assert d["state"] == b"AR"


def test_get():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)

    assert line.get("state") == b"AR"
    assert line["state"] == b"AR"


def test_str():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)

    assert line.str("state", "utf-8") == "AR"
    assert line.str("location", "utf-8").startswith("US")
    assert line.str("location", "utf-8").strip() == "US"


def test_int():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)

    assert line.int("birthday") == 19570526
    with pytest.raises(ValueError):
        assert line.int("state") == 0

    # Convert to int without first creating a string or stripping the string
    data = memoryview(b"US       AR10      Fbe56008be36eDianne Mcintosh         Whatever    Medic")
    line = FWFLine(fwf, 0, data)
    assert line.int("birthday") == 10

    data = memoryview(b"US       AR      20Fbe56008be36eDianne Mcintosh         Whatever    Medic")
    line = FWFLine(fwf, 0, data)
    assert line.int("birthday") == 20
