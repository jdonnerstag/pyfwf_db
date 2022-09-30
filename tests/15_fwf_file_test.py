#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name, missing-module-docstring
# pylint: disable=protected-access

# Current version of pylint not yet working well with python type hints and is causing plenty false positiv.
# pylint: disable=not-an-iterable, unsubscriptable-object

from typing import Iterable

import pytest

from fwf_db import FWFFile
from fwf_db import FWFLine
from fwf_db import FWFRegion
from fwf_db import FWFSubset
from fwf_db import fwf_open, op
from fwf_db.core.fwf_view_like import _FWFSort


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


class EmptyFileSpec:
    FIELDSPECS = []


def test_open():
    # We need both option:
    # 1) using a 'with' statement and automatically close again
    # 2) manually open and close it

    with fwf_open(HumanFile, DATA) as fwf:
        assert isinstance(fwf, FWFFile)
        assert fwf._mm is not None

    assert fwf._mm is None

    fd = fwf.open(DATA)
    assert fd is not None
    assert fd is fwf
    assert fd._mm is not None

    fd.close()
    assert fd._mm is None
    assert fwf._mm is None


def test_bytes_input():
    with fwf_open(HumanFile, DATA) as fwf:
        assert isinstance(fwf, FWFFile)
        assert fwf.encoding is None
        assert len(fwf.fields) == 8
        assert fwf.fwidth == 83
        assert fwf._fd is None
        assert fwf._mm is not None
        assert fwf.start_pos == 18
        assert fwf.line_count == 10
        assert fwf.count() == len(fwf) == 10


def test_initialize():
    with fwf_open(EmptyFileSpec, DATA, encoding="utf-8", comments="") as fwf:
        assert isinstance(fwf, FWFFile)
        assert fwf.encoding == "utf-8"
        assert len(fwf.fields) == 0         # The filespec is yet empty
        assert fwf.fwidth == 849            # Assume file size because of lack of details
        assert fwf.start_pos == 0           # Empty comment char => no comment char
        assert fwf.line_count == 0
        assert fwf.count() == len(fwf) == 0

        fwf.comments = "#"
        fwf.initialize()
        assert fwf.encoding == "utf-8"
        assert len(fwf.fields) == 0         # The filespec is yet empty
        assert fwf.fwidth == 831            # Excluding the comment line
        assert fwf.start_pos == 18          # Found the comment line
        assert fwf.line_count == 0
        assert fwf.count() == len(fwf) == 0

        fwf.add_field(name="location", len=9)
        fwf.initialize()
        assert len(fwf.fields) == 1
        assert fwf.fields["location"].slice == slice(0, 9)
        assert fwf.fwidth == 83
        assert fwf.start_pos == 18
        assert fwf.line_count == 10
        assert fwf.count() == len(fwf) == 10

        fwf.add_field(name="state", len=2)
        fwf.initialize()
        assert len(fwf.fields) == 2
        assert fwf.fields["location"].slice == slice(0, 9)
        assert fwf.fields["state"].slice == slice(9, 11)
        assert fwf.fwidth == 83
        assert fwf.start_pos == 18
        assert fwf.line_count == 10
        assert fwf.count() == len(fwf) == 10

        fwf.add_field(name="name", len=20)
        assert fwf.fields["location"].slice == slice(0, 9)
        assert fwf.fields["state"].slice == slice(9, 11)
        assert fwf.fields["name"].slice == slice(11, 31)
        #print(fwf.fields)

        fwf.update_field(name="name", start=20, len=10)
        assert fwf.fields["location"].slice == slice(0, 9)
        assert fwf.fields["state"].slice == slice(9, 11)
        assert fwf.fields["name"].slice == slice(20, 30)


def test_file_input():
    with fwf_open(HumanFile, "./sample_data/humans.txt") as fwf:
        assert isinstance(fwf, FWFFile)
        assert fwf.encoding is None
        assert len(fwf.fields) == 8
        assert fwf.fwidth == 83
        assert fwf._fd is not None
        assert fwf._mm is not None
        assert fwf.line_count == 10012
        assert fwf.start_pos == 0
        assert fwf.count() == len(fwf) == 10012


def test_table_iter():
    with fwf_open(HumanFile, DATA) as fwf:
        for rec in fwf:
            assert rec
            assert rec.line
            assert rec.lineno < 10 # we have 10 rows in our test data

        # Same with full file slice
        x = fwf[:]
        assert isinstance(x, Iterable)
        #for rec in fwf[:]:
        for rec in x:
            assert rec
            assert rec.line
            assert rec.lineno < 10 # we have 10 rows in our test data


def test_table_line_selector():
    with fwf_open(HumanFile, DATA) as fwf:
        rec1 = fwf.line_at(0)
        assert isinstance(rec1, FWFLine)
        assert rec1.lineno == 0
        assert rec1["birthday"] == b"19570526"

        rec1 = fwf[0]   # type: FWFLine
        assert isinstance(rec1, FWFLine)
        assert rec1.lineno == 0
        assert rec1["birthday"] == b"19570526"

        rec2 = fwf[0:1]
        assert isinstance(rec2, FWFRegion)
        assert slice(rec2.start, rec2.stop) == slice(0, 1)
        assert rec2.count() == len(rec2) == 1
        assert rec2.count() == len(list(rec2)) == 1
        for r in rec2:
            assert r["birthday"] in [b"19570526", b"19940213"]

        rec1 = fwf[5]
        assert isinstance(rec1, FWFLine)
        assert rec1.lineno == 5
        assert rec1["birthday"] == b"19770319"

        rec1 = fwf[-1]
        assert isinstance(rec1, FWFLine)
        assert rec1.lineno == 9
        assert rec1["birthday"] == b"20080503"

        rec2 = fwf[0:5]
        assert isinstance(rec2, FWFRegion)
        assert slice(rec2.start, rec2.stop) == slice(0, 5)
        assert rec2.count() == len(list(rec2)) == len(rec2)

        rec2 = fwf[-5:]
        assert isinstance(rec2, FWFRegion)
        assert slice(rec2.start, rec2.stop) == slice(5, 10)
        assert rec2.count() == len(list(rec2)) == len(rec2)

        rec2 = fwf[0:0]
        assert isinstance(rec2, FWFRegion)
        assert slice(rec2.start, rec2.stop) == slice(0, 0)
        assert rec2.count() == len(list(rec2)) == len(rec2)


def test_index_selector():
    with fwf_open(HumanFile, DATA) as fwf:
        # Unique on an index view
        rtn = fwf[0, 2, 5]
        assert isinstance(rtn, FWFSubset)
        assert rtn.count() == len(list(rtn)) == 3
        assert rtn.count() == len(rtn) == 3
        for r in rtn:
            assert r.rooted().lineno in [0, 2, 5]
            assert r["gender"] in [b"M", b"F"]

        rtn2 = fwf[0:6][0, 2, 5]
        assert isinstance(rtn2, FWFSubset)
        assert rtn.lines == rtn2.lines


def test_boolean_selector():
    with fwf_open(HumanFile, DATA) as fwf:
        # Unique on an boolean view
        rtn = fwf[True, False, True, False, False, True]
        assert rtn.count() == len(list(rtn)) == 3
        assert rtn.count() == len(rtn) == 3
        for r in rtn:
            assert r.rooted().lineno in [0, 2, 5]
            assert r["gender"] in [b"M", b"F"]


def test_multiple_selectors():
    with fwf_open(HumanFile, DATA) as fwf:
        rec = fwf[:]
        assert isinstance(rec, FWFRegion)
        assert slice(rec.start, rec.stop) == slice(0, 10)
        assert rec.count() == len(rec) == 10

        rec = rec[:]
        assert isinstance(rec, FWFRegion)
        assert slice(rec.start, rec.stop) == slice(0, 10)
        assert rec.count() == len(rec) == 10

        x = [1, 2, 3, 4, 5, 6]
        y = x[0:-1]
        assert 6 not in y

        rec = rec[0:-1]
        assert isinstance(rec, FWFRegion)
        assert slice(rec.start, rec.stop) == slice(0, 9)
        assert rec.count() == len(rec) == 9

        rec = rec[2:-2]
        assert isinstance(rec, FWFRegion)
        assert slice(rec.start, rec.stop) == slice(2, 7)
        assert rec.count() == len(rec) == 5

        with pytest.raises(Exception):
            rec = rec[1, 2, 4, 5]

        rec = rec[0, 1, 2, 4]
        assert isinstance(rec, FWFSubset)
        assert rec.lines == [0, 1, 2, 4]
        assert rec.count() == len(rec) == 4

        rec = rec[True, False, True]    # Implicit false, if True/False list is shorter
        assert isinstance(rec, FWFSubset)
        assert rec.lines == [0, 2]
        assert rec.count() == len(rec) == 2

        rec = fwf[:][:][0:-1][2:-2][1, 2, 4][True, False, True]
        assert isinstance(rec, FWFSubset)
        assert rec.lines == [0, 2]
        assert rec.count() == len(rec) == 2


def test_table_filter_by_line():
    with fwf_open(HumanFile, DATA) as fwf:
        rtn = fwf.filter(lambda l: l[19] == ord('F'))
        assert rtn.count() == len(list(rtn)) == 7
        assert rtn.count() == len(rtn) == 7

        rtn = fwf.exclude(lambda l: l[19] == ord('F'))
        assert rtn.count() == len(list(rtn)) == 3
        assert rtn.count() == len(rtn) == 3

        rtn = fwf.filter(lambda l: l[fwf.fields["gender"].start] == ord('M'))
        assert rtn.count() == len(list(rtn)) == 3
        assert rtn.count() == len(rtn) == 3

        rtn = fwf.filter(lambda l: l[fwf.fields["state"].slice] == b'AR')
        assert rtn.count() == len(list(rtn)) == 2
        assert rtn.count() == len(rtn) == 2

        rtn = fwf.filter(lambda l: l[fwf.fields["state"].slice] == b'XX')
        assert rtn.count() == len(list(rtn)) == 0
        assert rtn.count() == len(rtn) == 0

        # We often need to filter or sort by a date
        assert b"19000101" < b"20191231"
        assert b"20190101" < b"20190102"
        assert b"20190101" == b"20190101"
        assert b"20190102" > b"20190101"
        assert b"20200102" > b"20190101"


def test_table_filter_by_field():
    with fwf_open(HumanFile, DATA) as fwf:
        rtn = fwf.filter(op("gender") == b'F')              # "gender == 'F'"
        assert rtn.count() == len(list(rtn)) == 7

        rtn = fwf.filter(op("gender") == b'M')
        assert rtn.count() == len(list(rtn)) == 3           # "gender == 'M'"

        rtn = fwf.filter(op("gender").any([b'M', b'F']))    # "gender in ['M', 'F']"
        assert rtn.count() == len(list(rtn)) == 10

        rtn = fwf.filter(op("gender") == b'M', is_or=True)  # "gender == 'M'"
        assert rtn.count() == len(list(rtn)) == 3

        rtn = fwf.filter(op("gender") == b'F', op("state") == b'AR', is_or=True)    # "gender == 'M' or state == 'AR"
        assert rtn.count() == len(list(rtn)) == 7

        rtn = fwf.filter(op("gender") == b'F', op("state") == b'AR', is_or=False)   # "gender == 'F' or state == 'AR"
        assert rtn.count() == len(list(rtn)) == 2

        rtn = fwf.filter(op("name").str().strip().endswith("k"))           # "gender.strip.endswith('k')"
        assert rtn.count() == len(list(rtn)) == 2

        rtn = fwf.filter(op("name").str().upper().startswith("D"))         # "gender.upper.startswith('D')"
        assert rtn.count() == len(list(rtn)) == 1

        rtn = fwf.filter(op("state").str().contains("A"))           # "state.contains('M')"
        assert rtn.count() == len(list(rtn)) == 3

        rtn = fwf.filter(lambda line: op("birthday").str().date().get(line).year == 1957)
        assert rtn.count() == len(list(rtn)) == 1

        rtn = fwf.filter(op("birthday").bytes().startswith(b"1957"))    # "birthday.date.year == 1957"
        assert rtn.count() == len(list(rtn)) == 1

        rtn = fwf.filter(op("birthday")[0:4] == b"1957")    # "birthday.date.year == 1957"
        assert rtn.count() == len(list(rtn)) == 1

        fwf.add_field("birthday_year", start=11, len=4)
        rtn = fwf.filter(op("birthday_year") == b"1957")    # "birthday.date.year == 1957"
        assert rtn.count() == len(list(rtn)) == 1


def test_table_filter_by_function():
    with fwf_open(HumanFile, DATA) as fwf:
        gender = fwf.fields["gender"]
        state = fwf.fields["state"]

        def my_complex_reusable_test(line):
            # Not very efficient, but shows that you can do essentially everything
            rtn = (line[gender] == b'F') and str(line[state], "utf-8").startswith('A')
            return rtn

        rtn = fwf[:].filter(my_complex_reusable_test)
        assert rtn.count() == len(list(rtn)) == 2


def test_view_of_a_view():
    with fwf_open(HumanFile, DATA) as fwf:
        rec = fwf[1:8]
        assert fwf.fields == rec.fields
        assert rec.count() == len(list(rec)) == 7
        assert rec.count() == len(rec) == 7

        rtn = rec[2:4]
        assert rtn.count() == len(list(rtn)) == 2
        assert rtn.count() == len(rtn) == 2
        for rec in rtn:
            assert rec.rooted().lineno in [3, 4]

        rtn = fwf.filter_by_field("gender", b'F')
        assert rtn.count() == len(list(rtn)) == 7
        assert rtn.count() == len(rtn) == 7
        for rec in rtn:
            assert rec.rooted().lineno in [0, 3, 5, 6, 7, 8, 9]

        for rec in rtn[2:4]:
            assert rec.rooted().lineno in [5, 6]


def exec_empty_data(data):
    with fwf_open(HumanFile, data) as fwf:
        assert fwf.count() == len(fwf) == 0

        for _ in fwf:
            raise Exception("Should be empty")

        for _ in fwf.iter_lines():
            raise Exception("Should be empty")

        assert len(list(x for x in fwf)) == 0

        with pytest.raises(Exception):
            rtn = fwf[0:-1]

        rtn = fwf.filter_by_field("gender", b'F')
        assert rtn.count() == len(rtn) == 0


def test_empty_data():
    with pytest.raises(Exception):
        exec_empty_data(None)

    exec_empty_data(b"")
    exec_empty_data(b"# Empty")
    exec_empty_data(b"# Empty\n")


def test_sort():
    with fwf_open(HumanFile, DATA) as fwf:
        x = fwf.order_by("state")
        assert x.count() == fwf.count()
        #x.print(pretty=False)
        assert x[0].state == b'AR'
        assert x[0].birthday == b'19570526'
        assert x[1].state == b'AR'
        assert x[1].birthday == b'19820125'

        x = fwf.order_by("state", "-birthday")
        assert x.count() == fwf.count()
        #x.print(pretty=False)
        assert x[0].state == b'AR'
        assert x[0].birthday == b'19820125'
        assert x[1].state == b'AR'
        assert x[1].birthday == b'19570526'


def test_unique():
    with fwf_open(HumanFile, DATA) as fwf:
        x = fwf.unique("state")
        assert sorted(x) == [b'AR', b'MD', b'ME', b'MI', b'NV', b'OK', b'PA', b'VT', b'WI']

        x = sorted(fwf.unique("universe", "profession"))
        #print(x)
        assert x == [
            (b'Whatever    ', b'Comedian     '),
            (b'Whatever    ', b'Medic        '),
            (b'Whatever    ', b'Shark tammer '),
            (b'Whatever    ', b'Student      '),
            (b'Whatever    ', b'Super hero   '),
            (b'Whatever    ', b'Time traveler')
        ]


def test_lineno_line_file():
    with fwf_open(HumanFile, DATA) as fwf:
        data = fwf[5:]
        line = data[0]
        assert line._lineno == 5
        assert bytes(line._line).find(b"Richard Botto")
        assert line._file == "<literal>"

    with fwf_open(HumanFile, "sample_data/humans.txt") as fwf:
        data = fwf[5:]
        line = data[0]
        assert line._lineno == 5
        assert bytes(line._line).find(b"Richard Botto")
        assert line._file == "sample_data/humans.txt"

    with fwf_open(HumanFile, "sample_data/humans-subset.txt", "sample_data/humans.txt") as fwf:
        data = fwf[5:]
        line = data[0]
        assert line._lineno == 5
        assert bytes(line._line).find(b"Richard Botto")
        assert line._file == "sample_data/humans-subset.txt"


class HumanFileSpec:
    FIELDSPECS = [
            {"name": "name",       "slice": (32, 56)},
            {"name": "gender",     "slice": (19, 20)},
            {"name": "birthday",   "slice": (11, 19)},
        ]

    def __header__(self) -> list[str]:
        # Re-define the default for header
        return ["name", "gender", "birthday", "birthday_year", "age", "_lineno"]

    def birthday_year(self, line: FWFLine):
        return int(line["birthday"][0:4])

    def age(self, line: FWFLine):
        #return datetime.today().year - self.birthday_year(line)
        return 2021 - self.birthday_year(line)

    def __validate__(self, line: FWFLine) -> bool:
        rtn = (line._lineno % 2) != 0
        return rtn  # False => Error

    def __parse__(self, line: FWFLine):
        return line.to_dict()

    def my_comment_filter(self, line: FWFLine) -> bool:
        return line[0] != ord("#")


def test_computed_field():
    with fwf_open(HumanFileSpec, DATA) as fwf:
        line = fwf[0]
        assert line.name == b"Dianne Mcintosh         "
        assert line.gender == b"F"
        assert line.birthday == b"19570526"
        assert line.birthday_year == 1957
        assert line.age == 64
        assert line._lineno == 0

        # Use the __header__ configured to create the dict
        rtn = line.to_dict()
        assert rtn
        assert rtn["birthday_year"] == 1957
        assert rtn["age"] == 64
        assert rtn["_lineno"] == 0

        errors = fwf.validate()
        assert errors.count() == 5

        parsed_data = list(fwf.parse())
        assert parsed_data[0] == rtn

        assert line.validate() is False
        assert line.parse() == rtn


def test_get_string():
    with fwf_open(HumanFile, DATA) as fwf:
        data = list(fwf.to_list(header=False))
        assert len(fwf) == len(data)
        assert len(data) == 10
        for row in data:
            assert len(row) == 8    # Number of fields

        data = fwf.get_string(pretty=False, stop=5)
        #print(data)
        assert data

        data = fwf.get_string(pretty=True, stop=5)
        #print(data)
        assert data

        data = list(fwf.to_list("name", "state", "birthday", header=False, stop=5))
        assert len(data) == 5
        for row in data:
            assert len(row) == 3    # Number of fields

        data = fwf.get_string("name", "state", "birthday", pretty=False, stop=5)
        #print(data)
        assert data

        data = fwf.get_string("name", "state", "birthday", pretty=True, stop=5)
        #print(data)
        assert data

        data = list(fwf[0:5].to_list("name", "state", "birthday", header=False, stop=5))
        assert len(data) == 5
        for row in data:
            assert len(row) == 3    # Number of fields

        data = fwf[:5].get_string(pretty=False, stop=5)
        #print(data)
        assert data

        data = fwf[:5].get_string(pretty=True, stop=5)
        #print(data)
        assert data
