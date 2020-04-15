#!/usr/bin/env python
# encoding: utf-8

import pytest

import abc
import os
import sys
import io
from random import randrange
from time import time
from collections import defaultdict

from fwf_db.fwf_file import FWFFile
from fwf_db.cython import fwf_db_ext
from fwf_db.fwf_cython import FWFCython



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


def exec_fwf_cython_empty(data):

    fwf = FWFFile(HumanFile)
    with fwf.open(data) as fd:
        assert len(fd) == 0

        rtn = fwf_db_ext.fwf_cython(fwf, 
            -1, None, -1, None,
            -1, None, -1, None,
            index=None, 
            unique_index=False, 
            integer_index=False
        )
        assert len(fd) == len(rtn)

        rtn = FWFCython(fd).apply(-1, None, -1, None)
        assert len(fd) == len(rtn)


def test_fwf_cython_empty():

    exec_fwf_cython_empty(b"")
    exec_fwf_cython_empty(b"#")
    exec_fwf_cython_empty(b"# empty")
    exec_fwf_cython_empty(b"# empty \n")


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_fwf_cython_empty()
