#!/usr/bin/env python
# encoding: utf-8

"""
This is not a test file in the sense that you can/should execute it automatically.
It's rather a bunch of individual methods, mostly grown over time.

Run with python -m memory_profiler <script.py>
"""

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name, missing-module-docstring

import os
from time import time
from memory_profiler import profile
import numpy as np

from fwf_db import FWFFile
from fwf_db._cython import fwf_db_cython
from fwf_db import BytesDictWithIntListValues
from fwf_db import FWFIndexDict
from fwf_db.core import FWFSimpleIndexBuilder


class CENT_PARTY:

    FILE_PATTERN = r"DWH_TO_PIL_CENT_PARTY(_(VTAG|FULL|DELTA)).A901"

    FIELDSPECS = [
        {"name": "TRANCODE",            "len": 1, "dtype": "category", "regex": r" |A|M|D|X|N"},
        {"name": "PARTY_ID",            "len": 10},
        {"name": "ORG_NAME_1",          "len": 100},
        {"name": "ORG_NAME_2",          "len": 40},
        {"name": "ORG_NAME_3",          "len": 40},
        {"name": "ORG_STREET_NAME",     "len": 40},
        {"name": "ORG_HOUSE_NO",        "len": 40},
        {"name": "ORG_ZIP",             "len": 10},
        {"name": "ORG_CITY",            "len": 40},
        {"name": "SALES_LOCATION_ID",   "len": 10},
        {"name": "VALID_FROM",          "len": 8, "dtype": "int32", "default": 0},
        {"name": "VALID_UNTIL",         "len": 8, "dtype": "int32", "default": 21991231},
        {"name": "BUSINESS_DATE",       "len": 8, "dtype": "int32"},
    ]


class CENT_SALES_ASSIGNMENT:

    FILE_PATTERN = r"ANO_DWH..DWH_TO_PIL_CENT_SALES_ASSIGNMENT(_(VTAG|FULL|DELTA))?"

    FIELDSPECS = [
        {"name": "TRANCODE",            "len": 1, "dtype": "category", "regex": r" |A|M|D|X|N"},
        {"name": "SALES_LOCATION_ID",   "len": 10},
        {"name": "CONTRACT_HOLDER_ID",  "len": 10},
        {"name": "BAN",                 "len": 10},
        {"name": "MARKET_CODE",         "len": 3, "dtype": "category"},
        {"name": "CUSTOMER_NUMBER",     "len": 12},
        {"name": "CONTRACT_NUMBER",     "len": 12},
        {"name": "PRODUCT_COMMITMENT_NUMBER", "len": 12},
        {"name": "SALES_FORCE_ID",      "len": 8},
        {"name": "SALES_ORG_NUMBER",    "len": 22},
        {"name": "VALID_FROM",          "len": 8, "dtype": "int32", "default": 0},
        {"name": "VALID_UNTIL",         "len": 8, "dtype": "int32", "default": 21991231},
        {"name": "BUSINESS_DATE",       "len": 8, "dtype": "int32"},
    ]

    INDEX = "SALES_LOCATION_ID"

DIR = "../PIL-data-2/11_input_crlf_fixed/"
FILE_CENT_PARTY = DIR + "ANO_DWH..DWH_TO_PIL_CENT_PARTY_VTAG.20180119104659.A901"
FILE_SALES_ASSIGNMENT = DIR + "ANO_DWH..DWH_TO_PIL_CENT_SALES_ASSIGNMENT_VTAG.20180119115137.A901"


@profile
def test_default_dict():

    assert fwf_db_cython.say_hello_to("Susie") == "Hello Susie!"

    fwf = FWFFile(CENT_SALES_ASSIGNMENT)
    with fwf.open(FILE_SALES_ASSIGNMENT) as fd:
        assert len(fd) == 10363608

        rtn = FWFIndexDict(fwf)
        FWFSimpleIndexBuilder(rtn).index(fwf, "SALES_LOCATION_ID")
        assert len(rtn) == 3152698


@profile
def test_mem_optimized_dict():

    assert fwf_db_cython.say_hello_to("Susie") == "Hello Susie!"

    t1 = time()
    fwf = FWFFile(CENT_SALES_ASSIGNMENT)
    with fwf.open(FILE_SALES_ASSIGNMENT) as fd:
        print(f'Open file: {time() - t1} seconds')
        assert len(fd) == 10363608

        t1 = time()
        data = BytesDictWithIntListValues(len(fwf))
        rtn = FWFIndexDict(fwf, data)
        FWFSimpleIndexBuilder(rtn).index(fwf, "SALES_LOCATION_ID")
        print(f'Create index: {time() - t1} seconds')
        assert len(rtn) == 3152698

        t1 = time()
        data.finish()
        print(f'Finish index: {time() - t1} seconds')
        assert len(rtn) == 3152698


@profile
def test_mem_numpy():
    fsize = os.path.getsize(FILE_SALES_ASSIGNMENT)
    print(f"File size: {fsize:,}")
    #file = open(FILE_SALES_ASSIGNMENT, "rb")
    #all_of_it = file.read()

    fwf = FWFFile(CENT_SALES_ASSIGNMENT)
    with fwf.open(FILE_SALES_ASSIGNMENT) as fd:
    #with fwf.open(all_of_it) as fd:
        assert len(fd) == 10_363_608

        db = fwf_db_cython.field_data(fwf, "SALES_LOCATION_ID")
        assert len(db) == len(fd)      # PARTY_ID is unique in this file

        #index = dict.fromkeys(db)
        #assert len(index) == 3_152_698

        db_unique, db_counts = np.unique(db, return_counts=True)
        db_cumsum = np.cumsum(db_counts)
        index = dict.fromkeys(db_unique)
        #index = dict.fromkeys({db_unique[i]: db_cumsum[i] for i in range(len(db_unique))})
        assert len(index) == 3_152_698

        db = None   # It releases only the extra mem needed by field_data, but (correctly so) not the mmapped region.


if __name__ == "__main__":
    # Run with python -m memory_profiler script.py
    #test_default_dict()
    test_mem_numpy()
    #test_mem_optimized_dict()
