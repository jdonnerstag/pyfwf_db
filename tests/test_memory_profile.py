#!/usr/bin/env python
# encoding: utf-8

import pytest

import abc
import os
import sys
import io
from random import randrange
from time import time
import numpy as np
import pandas as pd
from collections import defaultdict
import ctypes

from fwf_db import FWFFile, FWFSimpleIndex, FWFMultiFile, FWFUnique
from fwf_db.fwf_np_unique import FWFUniqueNpBased
from fwf_db.fwf_np_index import FWFIndexNumpyBased
from fwf_db.fwf_cython_unique_index import FWFCythonUniqueIndex
from fwf_db.fwf_operator import FWFOperator as op
from fwf_db.fwf_cython import FWFCython
from fwf_db.cython import fwf_db_ext
from fwf_db.fwf_merge_index import FWFMergeIndex
from fwf_db._cython.fwf_mem_optimized_index import BytesDictWithIntListValues


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


@pytest.mark.slow
def test_default_dict():

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_SALES_ASSIGNMENT)

    with fwf.open(FILE_SALES_ASSIGNMENT) as fd:
        assert len(fd) == 10363608

        data = defaultdict(list)

        t1 = time()
        rtn = fwf_db_ext.fwf_cython(fwf,
            -1, None, -1, None,
            -1, None, -1, None,
            index="SALES_LOCATION_ID",
            unique_index=False,
            integer_index=False,
            index_dict=data,
            index_tuple=None
        )
        assert len(rtn) == 3152698


@pytest.mark.slow
def test_mem_optimized_dict():

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_SALES_ASSIGNMENT)

    with fwf.open(FILE_SALES_ASSIGNMENT) as fd:
        assert len(fd) == 10363608

        data = BytesDictWithIntListValues(len(fd))

        t1 = time()
        rtn = fwf_db_ext.fwf_cython(fwf,
            -1, None, -1, None,
            -1, None, -1, None,
            index="SALES_LOCATION_ID",
            unique_index=False,
            integer_index=False,
            index_dict=data,
            index_tuple=None
        )
        assert len(rtn) == 3152698


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    # pytest.main(["-v", "./tests"])

    # test_merge_unique_index()
    test_merge_non_unique_index()
