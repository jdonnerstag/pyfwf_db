#!/usr/bin/env python
# encoding: utf-8

import pytest

import os
import sys
import io
import numpy as np

from fwf_db import FWFFile, FWFSimpleIndex, FWFMultiFile, FWFUnique
from fwf_db.fwf_np_unique import FWFUniqueNpBased
from fwf_db.fwf_np_index import FWFIndexNumpyBased


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

        rtn = FWFUniqueNpBased(fwf).unique("state", dtype=(np.bytes_, 2))
        assert len(list(rtn)) == 9
        assert len(rtn) == 9

        rtn = FWFUniqueNpBased(fwf).unique("state", dtype="U2", func=lambda x: x.decode())
        assert len(list(rtn)) == 9
        assert len(rtn) == 9
        assert "MI" in rtn

        def to_str(x):
            return x.decode()

        rtn = FWFUniqueNpBased(fwf).unique("gender", dtype="U1", func=to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn

        # Unique on a view
        x = fwf[0:5]
        rtn = FWFUniqueNpBased(x).unique("gender", dtype="U1", func=to_str)
        assert len(list(rtn)) == 2
        assert len(rtn) == 2
        assert "M" in rtn
        assert "F" in rtn


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_unique_numpy()
