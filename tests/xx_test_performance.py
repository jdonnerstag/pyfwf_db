#!/usr/bin/env python
# encoding: utf-8

from time import time
import pytest

import abc
import os
import sys
import io
import datetime
from random import randrange
import numpy as np
#import pandas as pd
from collections import defaultdict
import ctypes
import inspect

from fwf_db import FWFFile, FWFSimpleIndex, FWFMultiFile, FWFUnique
#from fwf_db.fwf_unique_np_based import FWFUniqueNpBased
from fwf_db.fwf_np_index import FWFNumpyIndex
#from fwf_db.fwf_cython_unique_index import FWFCythonUniqueIndex
from fwf_db.fwf_operator import FWFOperator as op
#from fwf_db.fwf_cython import FWFCython
from fwf_db._cython import fwf_db_cython
#from fwf_db.fwf_merge_index import FWFMergeIndex
#from fwf_db._cython.fwf_mem_optimized_index import BytesDictWithIntListValues, MyIndexDict

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name, global-statement
# pylint: disable=

# ---------------------------------------------
# Performance Log
# ---------------------------------------------

LOG = None

def setup_module(module):
    """ setup any state specific to the execution of the given module."""
    global LOG
    LOG = open("./logs/performance.log", "at", encoding="utf-8")
    now = datetime.datetime.now()
    dt = now.strftime("%Y-%m-%d %H:%M:%S")
    LOG.write(f"{{ date: {dt}, ")


def teardown_module(module):
    """ teardown any state that was previously setup with a setup_module
    method.
    """
    LOG.write("}\n")  # type: ignore
    LOG.close()  # type: ignore


# ---------------------------------------------
# FWF specifications
# ---------------------------------------------

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

# -----------------------------------------------------------------------------
# Add additional Pytest marker configuration
# E.g. disable slow tests: '-m "not slow"
# Some tests run for 20+ seconds and we don't want them to run everytime
# -----------------------------------------------------------------------------

def log(t1, suffix=None):
    elapsed = time() - t1
    me = inspect.stack()[1][3]
    if suffix is not None:
        me = me + "-" + suffix
    LOG.write(f'{me}: {elapsed}, ')  # type: ignore
    print(f'{me}: Elapsed time is {elapsed} seconds.')

# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------

@pytest.mark.slow
def test_perf_iter_lines():
    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278   # The file is 2GB and has 5.8 mio records

        t1 = time()
        for i, line in fd.iter_lines():
            assert i >= 0
            assert line

        # Just read the lines (bytes)
        # 7.170794725418091     # My SDD shows almost no signs of being busy => CPU constraint
        log(t1)


@pytest.mark.slow
def test_perf_iter_fwfline():
    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        t1 = time()
        for line in fd:
            assert line

        # Wrap the line into a FWFLine object
        # 12.35709834098816     # 5-6 secs more (almost doubled the time)
        log(t1)


@pytest.mark.slow
def test_perf_simple_index():
    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278
        t1 = time()
        index = FWFSimpleIndex(fd).index("PARTY_ID")
        assert len(index) == 5889278    # No duplictates, which makes sense for this data
        log(t1, "Create_FWFSimpleIndex")

        idx = list(index.data.keys())
        t1 = time()
        for _ in range(int(1e6)):
            key =  randrange(len(index))
            key = idx[key]
            refs = index[key]
            assert refs

        # Create an index on PARTY_ID, using the optimized field reader (no FWFLine object)
        # Elapsed time is 22.193581104278564 seconds.   # 10 - 15 secs to create the index
        #
        # Access lines randomly 1 mio times
        # Elapsed time is 6.269562721252441 seconds.
        log(t1, "1mio_random_lookups")


@pytest.mark.slow
def test_numpy_samples():
    # Create 10 mio random string entries (10 bytes)
    reclen = int(10e6)
    values = np.empty(reclen, dtype="S10")

    t1 = time()
    flen = int(reclen / 15)
    for i in range(reclen):
        value = randrange(flen)
        value = f"{value:>10}"
        value = bytes(f"{value:>10}", "utf-8")
        values[i] = value

    log(t1, "Create_random_entries")        # Approx 16 secs

    t1 = time()
    data = defaultdict(list)
    all(data[value].append(i) or True for i, value in enumerate(values))
    log(t1, "Create-maps-of-lists-index")   # Approx 12 secs

    t1 = time()
    for i in range(int(1e6)):
        value = randrange(flen)
        value = f"{value:>10}"
        value = bytes(f"{value:>10}", "utf-8")

        rec = data.get(value, None)
        assert rec is not None

    log(t1, "1mio_random_lookups")      # Approx 5 secs


@pytest.mark.slow
def test_perf_numpy_index():

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278
        t1 = time()
        index = FWFNumpyIndex(fd).index("PARTY_ID")
        log(t1, "Create_FWFIndexNumpyBased")    # Approx 16 secs
        assert len(index) == 5889278    # No duplictates, which makes sense for this data

        idx = list(index.keys())
        t1 = time()
        for _ in range(int(1e6)):
            key =  randrange(len(index))
            key = idx[key]
            refs = index[key]
            assert refs

        log(t1, "1mio_random_lookups")      # Approx 4-5 secs

        # Create an index on PARTY_ID, using the optimized field reader (no FWFLine object).
        # A tiny bit faster then simple index
        #
        # Access lines randomly 1 mio times
        #
        # That is pretty much the same result, that the simple python based index provides.
        # Which makes sense, as both create dicts


@pytest.mark.slow
def test_effective_date_simple_filter():

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        t1 = time()
        fd = fd.filter(op("BUSINESS_DATE") < b"20180118")
        # They are dummy data with all the same change date !?!?
        assert len(fd) == 0   # type: ignore      # Approx 17 secs

        # A little slow, compared to 7 secs for simple bytes and 12 secs for FWFLines
        log(t1)


@pytest.mark.slow
def test_effective_date_region_filter():

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        t1 = time()
        # NOTE: you can not combine the operators with and resp. or
        fd = fd.filter(op("VALID_FROM") <= b"20130101")
        fd = fd.filter(op("VALID_UNTIL") >= b"20131231")  # type: ignore   # TODO: fix the signature
        assert len(fd) == 1293435

        # 29.993732929229736    # Compared to 17 secs for only 1 filter
        log(t1)


@pytest.mark.slow
def test_effective_date_region_filter_optimized():

    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        valid_from_slice = fd.fields["VALID_FROM"]
        valid_until_slice = fd.fields["VALID_UNTIL"]

        # Since "    " < "20130101" we don't need an extra test
        valid_until_last_pos = valid_until_slice.stop - 1

        def region_filter(line):
            if not (line[valid_from_slice] <= b"20130101"):
                return False

            if line[valid_until_last_pos] == 32:
                return True

            return line[valid_until_slice] >= b"20131231"

        t1 = time()
        fd = fd.filter(region_filter)
        assert len(fd) == 1293435  # type: ignore

        # 18.807480335235596    # Faster then then FWFOperator!!  But still CPU bound.
        log(t1)


@pytest.mark.slow
def test_cython_filter():
    fwf_db_cython.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        t1 = time()
        db = fwf_db_cython.FWFLineNumber(fwf)
        db.add_filter("BUSINESS_DATE", "20180118", upper=False)
        db.add_filter("VALID_FROM", "20130101", upper=False)
        db.add_filter("VALID_UNTIL", "20131231", upper=True)
        rtn = db.analyze()

        rlen = rtn.buffer_info()[1]
        assert rlen == 1293435
        log(t1)       # 2.1357336044311523   # Yes !!!!

'''
@pytest.mark.slow
def test_cython_get_field_data():

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        t1 = time()
        rtn = fwf_db_ext.get_field_data(fwf, "PARTY_ID")

        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')

        # In run mode:
        # 2.2793381214141846   # Cython really makes a difference


def my_find_last(data):
    _, indices = np.unique(data[::-1], return_index=True)
    return indices[::-1]


@pytest.mark.slow
def test_find_last():
    """This is an interesting test case as often we need the last record
    before the effective date, ...."""

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        t1 = time()
        rtn = fwf_db_ext.create_unique_index(fwf, "PARTY_ID")
        is_unique = len(rtn) == len(fd)
        print(f'Elapsed time is {time() - t1} seconds. {len(rtn):,d} - {"unique" if is_unique else "not unique"} index')

        # In run mode:
        # 5.535429954528809   # Ok

        """
        t1 = time()
        rtn = fwf_db_ext.get_field_data(fwf, "PARTY_ID")
        print(f'Elapsed time is {time() - t1} seconds.')

        indices = my_find_last(rtn)
        is_unique = len(indices) == len(fd)
        print(f'Elapsed time is {time() - t1} seconds. {len(indices):,d} - {"unique" if is_unique else "not unique"} index')
        # Until here it is nice and really fast (4-5 secs), but ...
        # either we convert it into a dict for search, which takes several secs.,
        # because Numpy/Pandas based search on this specific data is really really slow.
        # But if new need a dict for perf anyways, then we can create it right away
        # without the numpy array in between

        df = pd.DataFrame(indices, columns=["values"])
        df["index"] = df.index
        df = df.set_index("values")

        t1 = time()
        for _ in range(int(1e6)):
            key =  randrange(len(rtn))
            key = df.iloc[key]
            refs = df.loc[key]
            assert refs is not None

        print(f'Elapsed time is {time() - t1} seconds.')

        # In run mode:
        # 4.8622496128082275   # Nice, but search is teribly slow.
        """

        """
        values = {value : i for i, value in enumerate(rtn)}
        is_unique = len(values) == len(fd)
        print(f'Elapsed time is {time() - t1} seconds. {len(values):,d} - {"unique" if is_unique else "not unique"} index')

        # In run mode:
        # 9.488121032714844   # 6 secs to create the index
        """

        """
        df = pd.DataFrame(rtn, columns=["values"])
        df = df.groupby("values").tail(1)
        is_unique = len(df.index) == len(fd)
        print(f'Elapsed time is {time() - t1} seconds. {len(df.index):,d} - {"unique" if is_unique else "not unique"} index')

        # In run mode:
        # 22.59328055381775   # 19 secs to create the index
        """


@pytest.mark.slow
def test_numpy_sort():

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        t1 = time()
        rtn = fwf_db_ext.get_field_data(fwf, "PARTY_ID")
        print(f'Elapsed time is {time() - t1} seconds.')

        #rtn = np.argsort(rtn)
        rtn = np.argsort(rtn)
        print(f'Elapsed time is {time() - t1} seconds. {len(rtn):,d}')

        # In run mode:
        # 4.589160680770874   # just 2.2 secs to sort


@pytest.mark.slow
def test_cython_create_index():

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        """ """
        t1 = time()
        rtn = fwf_db_ext.create_index(fwf, "PARTY_ID")

        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')

        # In run mode:
        # 12.35611081123352    # A little better, but not much
        """ """

        """
        t1 = time()
        rtn = fwf_db_ext.get_field_data(fwf, "PARTY_ID")
        print(f'Elapsed time is {time() - t1} seconds.')

        values = defaultdict(list)
        all(values[value].append(i) or True for i, value in enumerate(rtn))
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')

        # 16.387330770492554   # Reading the data is really fast, but creating the index is not yet.
        #                      # Accessing the index afterwards is very fast
        """

        """
        t1 = time()
        rtn = fwf_db_ext.get_field_data(fwf, "PARTY_ID")
        rtn_hash = [hash(a) for a in rtn]
        print(f'Elapsed time is {time() - t1} seconds.')

        def find(value):
            return np.argwhere(rtn_hash == hash(value))

        idx = np.unique(rtn)

        t1 = time()
        for i in range(int(1e6)):
            key =  randrange(len(idx))
            key = idx[key]
            refs = find(key)
            assert refs is not None

        print(f'Elapsed time is {time() - t1} seconds.')

        # 4.449473857879639 sec to prepare
        # 28.354421854019165 for 1 mio searches (compared ro 3 secs with a dict)
        """

@pytest.mark.slow
def test_int_index():
    # Let's assume we know the PK field are integers only

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        """
        t1 = time()
        rtn = fwf_db_ext.create_index(fwf, "PARTY_ID")

        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')

        # In run mode:
        # 12.35611081123352    # A little better, but not much
        """

        """
        t1 = time()
        rtn = fwf_db_ext.get_int_field_data(fwf, "PARTY_ID")
        print(f'Elapsed time is {time() - t1} seconds.')

        #t1 = time()
        #for _ in range(int(1e6)):
        #    key =  randrange(len(rtn))
        #    key = rtn[key]
        #    refs = np.argwhere(rtn == key)
        #    assert refs is not None
        # print(f'Elapsed time is {time() - t1} seconds.')

        values = defaultdict(list)
        all(values[value].append(i) or True for i, value in enumerate(rtn))
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')

        # 2.6446585655212402    # Converting bytes into int64 makes almost no
        #                       # difference in reading the field data
        # Searching int an int64 numpy array is still sloooww
        # 13.195310354232788    # Creating a dict with int is slightly faster
        """

        """ """
        t1 = time()
        rtn = fwf_db_ext.create_int_index(fwf, "PARTY_ID")
        print(f'Elapsed time is {time() - t1} seconds.')

        # 11.042781352996826 secs   # marginably faster
        """ """


@pytest.mark.slow
def test_fwf_cython():

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        t1 = time()
        rtn = fwf_db_ext.fwf_cython(fwf,
            -1, None, -1, None,
            -1, None, -1, None,
            index=None,
            unique_index=False,
            integer_index=False
        )
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')
        assert len(fd) == len(rtn)

        # In run mode:
        # 0.05099773406982422

        rtn = fwf_db_ext.fwf_cython(fwf,
            fd.fields["BUSINESS_DATE"].start, b"20180118",
            -1, None,
            fd.fields["VALID_FROM"].start, b"20130101",
            fd.fields["VALID_UNTIL"].start, b"20131231",
            index=None,
            unique_index=False,
            integer_index=False
        )

        rlen = rtn.buffer_info()[1]
        print(f'Elapsed time is {time() - t1} seconds.    {rlen:,d}')
        assert rlen == 1293435

        # In run mode:
        # 1.989365816116333

        t1 = time()
        rtn = fwf_db_ext.fwf_cython(fwf,
            -1, None, -1, None,
            -1, None, -1, None,
            index="PARTY_ID",
            unique_index=False,
            integer_index=False
        )
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')
        assert len(fd) == len(rtn)

        # In run mode:
        # 11.820953607559204

        t1 = time()
        rtn = fwf_db_ext.fwf_cython(fwf,
            -1, None, -1, None,
            -1, None, -1, None,
            index="PARTY_ID",
            unique_index=True,
            integer_index=False
        )
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')
        assert len(fd) == len(rtn)

        # In run mode:
        # 4.296884775161743

        t1 = time()
        rtn = fwf_db_ext.fwf_cython(fwf,
            -1, None, -1, None,
            -1, None, -1, None,
            index="PARTY_ID",
            unique_index=True,
            integer_index=True
        )
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')
        assert len(fd) == len(rtn)

        # In run mode:
        # 3.4603333473205566

        t1 = time()
        rtn = fwf_db_ext.fwf_cython(fwf,
            -1, None, -1, None,
            -1, None, -1, None,
            index="PARTY_ID",
            unique_index=False,
            integer_index=True
        )
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')
        assert len(fd) == len(rtn)

        # In run mode:
        # 10.110804796218872


@pytest.mark.slow
def test_merge_unique_index():

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5889278

        data = defaultdict(list)

        t1 = time()
        rtn = fwf_db_ext.fwf_cython(fwf,
            -1, None, -1, None,
            -1, None, -1, None,
            index="PARTY_ID",
            unique_index=False,
            integer_index=False,
            index_dict=data,
            index_tuple=None
        )
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')
        assert len(fd) == len(rtn)

        # Non-unique index with defaultdict
        # 14.49066162109375 seconds

        data = dict()

        t1 = time()
        rtn = fwf_db_ext.fwf_cython(fwf,
            -1, None, -1, None,
            -1, None, -1, None,
            index="PARTY_ID",
            unique_index=True,
            integer_index=False,
            index_dict=data,
            index_tuple=None
        )
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')
        assert len(fd) == len(rtn)

        # Unique index with plain dict
        # 5.222317934036255 seconds

        data = BytesDictWithIntListValues(len(fd))

        t1 = time()
        rtn = fwf_db_ext.fwf_cython(fwf,
            -1, None, -1, None,
            -1, None, -1, None,
            index="PARTY_ID",
            unique_index=False,
            integer_index=False,
            index_dict=data,
            index_tuple=None
        )
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')
        assert len(fd) == len(rtn)

        # non-unique index with mem optimized dict
        # 6.351483106613159 seconds   # only little overhead, and faster then defaultdict :)


@pytest.mark.slow
def test_merge_non_unique_index():

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
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')
        assert len(rtn) == 3152698

        # Non-unique index with defaultdict
        # 18.443543195724487 seconds

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
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')
        assert len(rtn) == 3152698

        # non-unique index with mem optimized dict
        # 11.132811546325684 seconds   # Not bad. Faster then defaultdict.


# @pytest.mark.slow
# TODO This test is not yet implemented
def test_MyIndexDict_get():

    print("Test started")
    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        rec_size = len(fd)
        assert rec_size == 5889278

        field_pos = fd.fields["PARTY_ID"].start
        field_len = fd.fields["PARTY_ID"].stop - fd.fields["PARTY_ID"].start

        data = MyIndexDict(size=rec_size, mm=fd.mm, reclen=fd.reclen, field_pos=field_pos, field_len=field_len, align="right")
        assert data

        # Get list of keys and timeit
        t1 = time()
        gen = ((i, line[fd.fields["PARTY_ID"]]) for i, line in fd.iter_lines())
        for i, key in gen:
            pass
        print(f'1. Elapsed time is {time() - t1} seconds.')

        # Put the keys into the index
        t1 = time()
        gen = ((i, line[fd.fields["PARTY_ID"]]) for i, line in fd.iter_lines())
        for i, key in gen:
            data.put(key, i)
        percentage_filled, buckets_by_length, max_length, buckets_length = data.analyze()
        print(f"precentage filled: {percentage_filled}, max_length: {max_length}, distibution: {buckets_by_length}")
        print(f'2. Elapsed time is {time() - t1} seconds.')

        # TODO randomly pick value from list for 1 mio get() and timeit.

        # TODO 1 mio get() with "key not found"

        # TODO randomly pick key and every 10th is not-found

        # TODO Play with "capacity" and compare performance results

        # TODO if possible compare key length < 8 bytes, == 8 bytes and > 8 bytes

'''
