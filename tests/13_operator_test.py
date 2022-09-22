#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name

from fwf_db import FWFFile
from fwf_db import FWFOperator as op


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



def test_table_filter_by_line():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        rtn = fwf.filter(op("gender") == b"M")
        assert rtn.count() == len(list(rtn)) == 3
        assert rtn.count() == len(rtn) == 3

        rtn = fwf.filter(op("gender") != b"M")
        assert rtn.count() == len(list(rtn)) == 7
        assert rtn.count() == len(rtn) == 7

        rtn = fwf.filter(op("gender").bytes() > b"F")
        assert rtn.count() == len(list(rtn)) == 3
        assert rtn.count() == len(rtn) == 3

        rtn = fwf.filter(op("gender").bytes() < b"M")
        assert rtn.count() == len(list(rtn)) == 7
        assert rtn.count() == len(rtn) == 7

        rtn = fwf.filter(op("gender").str() == "F")
        assert rtn.count() == len(rtn) == 7

        rtn = fwf.filter(op("gender").str().strip() == "F")
        assert rtn.count() == len(rtn) == 7

        rtn = fwf.filter(op("gender").str().lower() == "f")
        assert rtn.count() == len(rtn) == 7

        rtn = fwf.filter(op("birthday").int() < 19600000)
        assert rtn.count() == len(rtn) == 2

        birthday_year = op("birthday")
        birthday_year.func = lambda x: int(x) / 100 / 100
        rtn = fwf.filter(birthday_year < 1960)

        birthday_year = op("birthday", lambda x: int(x) / 100 / 100)
        rtn = fwf.filter(birthday_year < 1960)
