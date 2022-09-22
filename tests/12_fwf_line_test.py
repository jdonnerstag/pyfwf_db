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
        {"name": "unknown", "len": 12},
        {"name": "name", "len": 24},
        {"name": "universe", "len": 12},
        {"name": "profession", "len": 13},
        {"name": "dummy", "len": 1},
    ]


def test_constructor():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)
    assert line
    assert line.lineno == 0
    assert line.line == DATA
    assert line.get_line() == DATA


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

    assert len(d) == 9
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


def test_getattr():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)
    assert line.state == b"AR"
    assert line.location == b"US       "
    assert line.state.decode() == "AR"

    with pytest.raises(AttributeError):
        _ = line.statexxx


def test_iter():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)
    assert line.keys()
    it = iter(line.items())
    assert next(it) == ("location", b"US       ")
    assert next(it) == ("state", b"AR")
    assert next(it) == ("birthday", b"19570526")
    assert next(it) == ("gender", b"F")
    assert next(it) == ("unknown", b"be56008be36e")
    assert next(it) == ("name", b"Dianne Mcintosh         ")
    assert next(it) == ("universe", b"Whatever    ")
    assert next(it) == ("profession", b"Medic        ")
    assert next(it) == ("dummy", b"#")

    with pytest.raises(StopIteration):
        next(it)

    assert tuple(line) == (
        b"US       ",
        b"AR",
        b"19570526",
        b"F",
        b"be56008be36e",
        b"Dianne Mcintosh         ",
        b"Whatever    ",
        b"Medic        ",
        b"#"
    )


def test_rooted():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)
    x = line.rooted()
    assert x.fwf_view == line.fwf_view == fwf
    assert x.lineno == line.lineno == 0
    assert x.line == line.line == DATA


def test_get_string():
    fwf = FWFFile(HumanFile)
    line = FWFLine(fwf, 0, DATA)
    _ = line.get_string(pretty=True)
    _ = line.get_string("location", "state", "birthday", pretty=True)
    _ = line.get_string(pretty=False)
    _ = line.get_string("location", "state", "birthday", pretty=False)

    with pytest.raises(AttributeError):
        _ = line.get_string("does not exist")
