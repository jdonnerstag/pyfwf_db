#!/usr/bin/env python
# encoding: utf-8

import pytest

import os
import sys
import io
import numpy as np

from fwf_db import FWFFile
from fwf_db.fwf_pandas import FWFPandas
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


def test_pandas():
    fwf = FWFFile(HumanFile)
    with fwf.open(DATA):

        df = FWFPandas(fwf).to_pandas()
        assert len(df.index) == 10
        assert len(df.columns) == 8
        assert list(df.columns) == list(fwf.fields.keys())


def exec_pandas_empty(data):
    fwf = FWFFile(HumanFile)
    with fwf.open(data):

        df = FWFPandas(fwf).to_pandas()
        assert len(df.index) == 0


def test_pandas_empty():

    exec_pandas_empty(b"")
    exec_pandas_empty(b"#")
    exec_pandas_empty(b"# Empty")
    exec_pandas_empty(b"# empty\n")


def exec_cython_empty(data):
    fwf = FWFFile(HumanFile)
    with fwf.open(data) as fd:
        data = FWFCython(fd).apply()
        df = FWFPandas(data).to_pandas()

    assert len(df.index) == 0


def test_pandas_empty_cython():

    exec_cython_empty(b"")
    exec_cython_empty(b"#")
    exec_cython_empty(b"# Empty")
    exec_cython_empty(b"# empty\n")


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_pandas()
