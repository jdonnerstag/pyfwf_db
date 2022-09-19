#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name, missing-module-docstring

"""
This is not a test file in the sense that you can/should execute it automatically.
It's rather a bunch of individual methods, mostly grown over time.

Run individual tests like:
  pytest -svx -m "slow" <script.py>::<func name>
"""

from io import TextIOWrapper
from itertools import islice
from random import randrange
from time import time
from collections import defaultdict
import datetime
import inspect

import numpy as np
import pandas as pd

import pytest

from fwf_db.fwf_file import FWFFile
from fwf_db.fwf_dict import FWFDict
from fwf_db.fwf_line import FWFLine
from fwf_db.fwf_np_index import FWFNumpyIndexBuilder
from fwf_db.fwf_operator import FWFOperator as op
from fwf_db._cython import fwf_db_cython
from fwf_db.fwf_index_like import FWFIndexDict, FWFUniqueIndexDict
from fwf_db.fwf_simple_index import FWFSimpleIndexBuilder
from fwf_db.fwf_cython_index import FWFCythonIndexBuilder
from fwf_db._cython.fwf_mem_optimized_index import BytesDictWithIntListValues

# ---------------------------------------------
# Performance Log
# ---------------------------------------------

LOG = None

# Pytest fixture to initialize the module
def setup_module(module):   # pylint: disable=unused-argument
    """ setup any state specific to the execution of the given module."""
    global LOG      # pylint: disable=global-statement
    LOG = open("./logs/performance.log", "at", encoding="utf-8")
    now = datetime.datetime.now()
    dt = now.strftime("%Y-%m-%d %H:%M:%S")
    LOG.write(f"{{ date: {dt}, ")


# Pytest fixture to teardown the module
def teardown_module(module):    # pylint: disable=unused-argument
    """ teardown any state that was previously setup with a setup_module
    method.
    """
    assert isinstance(LOG, TextIOWrapper)
    LOG.write("}\n")
    LOG.close()


def log(t1, suffix=None):
    elapsed = time() - t1
    me = inspect.stack()[1][3]
    if suffix is not None:
        me = me + "-" + suffix

    if LOG is not None:
        assert isinstance(LOG, TextIOWrapper)

        LOG.write(f'{me}: {elapsed}, ')

    print(f'{me}: Elapsed time is {elapsed} seconds.')


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
# We've added a Pytest marker configuration "slow"
# Some tests run for 20+ seconds and we don't want them to run everytime.
# They are disabled by default: see ./pyproject.toml
# Enable "slow" tests with: pytest -m slow ...
# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------

@pytest.mark.slow
def test_perf_iter_lines():

    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278   # The file is 2GB and has 5.8 mio records

        # Just read line by line and return the bytes
        t1 = time()
        i = 0
        for i, line in enumerate(fd.iter_lines()):
            assert line

        assert (i + 1) == len(fd)

        # Between 2.88 and 8 secs (first invocation)
        # My SDD shows almost no signs of being busy => CPU constraint
        log(t1)


@pytest.mark.slow
def test_perf_iter_fwfline():

    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        # Same as above, except that every line gets wrapped into a FWFLine object
        t1 = time()
        line = None
        for line in fd:
            assert line

        assert isinstance(line, FWFLine)
        assert (line.lineno + 1) == len(fd)

        # Between 7 and 12 secs. Approx 4-5 secs more!!!
        log(t1)


def access_index_many_time(index, count, logmsg):
    idx = list(index.keys())   # dict[Any, list[int]]
    len_index = len(idx)
    t1 = time()
    for _ in range(int(count)):
        key =  randrange(len_index)
        key = idx[key]
        refs = index[key]
        assert refs

    # Elapsed time is 6.269562721252441 seconds.
    log(t1, logmsg)


def exec_perf_index(index_dict, index_builder, log_1, log_2):

    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        # Create an index on PARTY_ID, using the optimized field reader (no FWFLine object)
        t1 = time()
        index = index_dict(fd)
        index_builder(index).index(fd, "PARTY_ID")
        assert len(index) == 5_889_278    # PARTY_ID has no duplicates

        # 10 - 15 secs to create the index
        log(t1, log_1)

        # Access lines randomly 1 mio times
        # Elapsed time is 6.269562721252441 seconds.
        access_index_many_time(index, 1e6, log_2)


@pytest.mark.slow
def test_perf_simple_index():
    exec_perf_index(FWFIndexDict, FWFSimpleIndexBuilder, "FWFSimpleIndexBuilder", "FWFIndexDict_1mio_random_lookups")
    exec_perf_index(FWFIndexDict, FWFNumpyIndexBuilder, "FWFNumpyIndexBuilder", "FWFIndexDict_1mio_random_lookups")
    exec_perf_index(FWFIndexDict, FWFCythonIndexBuilder, "FWFCythonIndexBuilder", "FWFIndexDict_1mio_random_lookups")


@pytest.mark.slow
def test_numpy_samples():
    # Preparation: create 10 mio random string entries (10 bytes)
    reclen = int(10e6)
    values = np.empty(reclen, dtype="S10")

    t1 = time()
    flen = int(reclen / 15)     # Make sure we create some duplicates
    for i in range(reclen):
        value = randrange(flen)
        value = bytes(f"{value:>10}", "utf-8")
        values[i] = value

    # Approx 16 secs
    log(t1, "Preparation-Create_random_entries")

    # Create an "index" with defaultdict(list) (dict[Any, list[int]])
    t1 = time()
    data = defaultdict(list)
    all(data[value].append(i) or True for i, value in enumerate(values))
    log(t1, "Create-defaultlist(list)-index")   # Approx 8.2 secs

    # Approx 1.2 secs
    access_index_many_time(data, 1e6, "defaultdict_1mio_random_lookups")

    # Create an "index" with FWFDict (dict[Any, list[int]])
    t1 = time()
    data = FWFDict()
    for i, value in enumerate(values):
        data[value] = i
    log(t1, "Create-FWFDict-index")

    # A tiny bit slower then defaultdict(list) !!!
    access_index_many_time(data, 1e6, "FWFDict_1mio_random_lookups")


@pytest.mark.slow
def test_effective_date_simple_filter():
    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        t1 = time()
        fd = fd.filter(op("BUSINESS_DATE") < b"20180118")
        # They are dummy data with all the same change date !?!?
        assert len(fd) == 0

        # 18 secs. A little slow, compared to 7 secs for simple bytes and 12 secs for FWFLines
        log(t1)


@pytest.mark.slow
def test_effective_date_region_filter():
    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        t1 = time()
        # NOTE: you cannot combine the operators with AND resp. OR. It is always AND.
        # Identify all entries which started at or before 20130101 and ended at or after 20131231
        fd = fd.filter(op("VALID_FROM") <= b"20130101")
        fd = fd.filter(op("VALID_UNTIL") >= b"20131231")
        assert len(fd) == 1_293_435

        # 21 secs, compared to 18 secs for only 1 filter
        log(t1)


@pytest.mark.slow
def test_effective_date_region_filter_optimized():
    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

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
        assert len(fd) == 1_293_435

        # 20 sec; No differnce with FWFOperators
        log(t1)


@pytest.mark.slow
def test_cython_filter():
    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        filters = fwf_db_cython.FWFFilters(fwf)
        #filters.add_filter_2("BUSINESS_DATE", "20180118", upper=False)
        #filters.add_filter_2("VALID_FROM", "20130101", upper=True)
        filters.add_filter_2("VALID_FROM", "20130101", upper=True, equal=True)  # Note: lower <= x < upper
        filters.add_filter_2("VALID_UNTIL", "20131231", upper=False, equal=False)

        t1 = time()
        rtn = fwf_db_cython.line_numbers(fwf, filters=filters)
        assert len(rtn) == 1_293_435
        log(t1)       # 1.2 secs   # Yes !!!!


@pytest.mark.slow
def test_cython_get_field_data():
    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        t1 = time()
        rtn = fwf_db_cython.field_data(fwf, "PARTY_ID")
        print(f'Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')

        # In run mode:
        # 1.85 secs, Cython really makes a difference


def my_find_last(data):
    _, indices = np.unique(data[::-1], return_index=True)
    return indices[::-1]


@pytest.mark.slow
def test_find_last():
    """
    Finding the last in a numpy array or pandas dataframe is possible
    and is reasonably fast, but why load all data in the first place.
    With a normal dict it is actually straight forward. The last will
    simply replace the previous one.
    """

    print("")

    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        # PARTY_ID is unique. As long as a standard dict is used, the performance is not different.
        t1 = time()
        #data = BytesDictWithIntListValues(len(fwf))    # 20 secs
        index = FWFUniqueIndexDict(fwf)                 # 8.6 secs
        #index = FWFIndexDict(fwf)                      # 25 secs
        fwf_db_cython.create_index(fwf, "PARTY_ID", index)
        is_unique = len(index) == len(fd)
        print(f'1. Elapsed time is {time() - t1} seconds. {len(index):,d} - {"unique" if is_unique else "not unique"} index')

        # approx 1.3 secs out of the numbers above, are required for retrieving the data
        t1 = time()
        rtn = fwf_db_cython.field_data(fwf, "PARTY_ID")
        print(f'2. Elapsed time is {time() - t1} seconds.')

        # field_data() returns a numpy array. Use numpy to determine the last for each key.
        # Since PARTY_ID is unique, the return array is unchanged.
        # approx 3-4 secs
        indices = my_find_last(rtn)
        is_unique = len(indices) == len(fd)
        print(f'3. Elapsed time is {time() - t1} seconds. {len(indices):,d} - {"unique" if is_unique else "not unique"} index')

        # Searching these data in Numpy/Pandas is really slow.
        # 4.7 secs for only 10_000 lookups !!
        df = pd.DataFrame(indices, columns=["values"])
        df["index"] = df.index
        df = df.set_index("values")

        t1 = time()
        for _ in range(int(10_000)):
            key =  randrange(len(rtn))
            key = df.iloc[key]
            refs = df.loc[key]
            assert refs is not None

        print(f'4. Elapsed time is {time() - t1} seconds.')

        # As Numpy/Pandas based search on these data is really slow, try and
        # approach based on python standard dicts.
        # If that works well, then we can create it right away without the
        # numpy array in between

        # 7.9 secs to create a standard dict (unique index)
        values = {value : i for i, value in enumerate(rtn)}
        is_unique = len(values) == len(fd)
        print(f'5. Elapsed time is {time() - t1} seconds. {len(values):,d} - {"unique" if is_unique else "not unique"} index')

        # First we tested Numpy to create the index, then a dict, and now Pandas groupby.
        # approx 22 secs
        df = pd.DataFrame(rtn, columns=["values"])
        df = df.groupby("values").tail(1)
        is_unique = len(df.index) == len(fd)
        print(f'6. Elapsed time is {time() - t1} seconds. {len(df.index):,d} - {"unique" if is_unique else "not unique"} index')



@pytest.mark.slow
def test_numpy_sort():
    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        t1 = time()
        rtn = fwf_db_cython.field_data(fwf, "PARTY_ID")
        print(f'Elapsed time is {time() - t1} seconds.')

        rtn = np.argsort(rtn)
        print(f'Elapsed time is {time() - t1} seconds. {len(rtn):,d}')

        # approx 2-3 secs to sort


@pytest.mark.slow
def test_cython_create_index():
    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        # Create a non-unique index with defaultdict(list).
        # PARTY_ID is unique, hence the test does not consider the non-unique use cases
        # 2-3 secs to read the data, and approx 16 secs overall
        t1 = time()
        index = FWFIndexDict(fwf)
        rtn = fwf_db_cython.field_data(fwf, "PARTY_ID", index)
        print("")
        print(f'1. Elapsed time is {time() - t1} seconds.')

        values = defaultdict(list)
        all(values[value].append(i) or True for i, value in enumerate(rtn))
        print(f'2. Elapsed time is {time() - t1} seconds.    {len(rtn):,d}')

        # Create a non-unique Index, with close to standard FWFDict underneath.
        # FWFDict is similar to defaultdict(list), but the same
        # Approx 14 secs
        t1 = time()
        index = FWFIndexDict(fwf, FWFDict())
        fwf_db_cython.create_index(fwf, "PARTY_ID", index)
        print(f'3. Elapsed time is {time() - t1} seconds.    {len(index):,d}')

        # Create a unique Index, with standard dict
        # Approx 7 secs  (which is much faster then the non-unique one)
        t1 = time()
        index = FWFUniqueIndexDict(fwf)
        fwf_db_cython.create_index(fwf, "PARTY_ID", index)
        print(f'4. Elapsed time is {time() - t1} seconds.    {len(index):,d}')

        # Determine how long it takes to create the hashes for all the entries
        # Approx 3-4 secs.  Which is significant compared to the full time needed.
        # TODO Didn't start on a FWF specific hash implementation which is faster??
        t1 = time()
        index = FWFIndexDict(fwf)
        rtn = fwf_db_cython.field_data(fwf, "PARTY_ID")
        rtn_hash = [hash(a) for a in rtn]
        print(f'5. Elapsed time is {time() - t1} seconds.')

        # "Lookup" numpy arrays => fairly slow => no alternative for dicts.
        def find(value):
            return np.argwhere(rtn_hash == hash(value))

        idx = np.unique(rtn)

        t1 = time()
        for i in range(int(1e6)):
            key =  randrange(len(idx))
            key = idx[key]
            refs = find(key)
            assert refs is not None

        print(f'6. Elapsed time is {time() - t1} seconds.')


@pytest.mark.slow
def test_int_index():
    # Let's assume we know the PK field are integers only

    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        # Create a non-unique INT index with defaultdict(list).
        # PARTY_ID is unique, hence the test does not consider the non-unique use cases
        # 2-3 secs to read the data, and approx 16 secs overall
        # Only XYZ secs slower then string index keys
        t1 = time()
        index = FWFIndexDict(fwf)
        fwf_db_cython.create_index(fwf, "PARTY_ID", index, func="int")
        print("")
        print(f'1. Elapsed time is {time() - t1} seconds.    {len(index):,d}')

        # Same as above, but INT index
        # Approx XYZ secs
        t1 = time()
        index = FWFUniqueIndexDict(fwf)
        fwf_db_cython.create_index(fwf, "PARTY_ID", index, func="int")
        print(f'2. Elapsed time is {time() - t1} seconds.')

        # Create a defaultdict(list) with integer keys
        rtn = fwf_db_cython.field_data(fwf, "PARTY_ID")
        values = defaultdict(list)
        all(values[int(value)].append(i) or True for i, value in enumerate(rtn))
        print(f'3. Elapsed time is {time() - t1} seconds.    {len(index):,d}')


@pytest.mark.slow
def test_merge_unique_index():
    # PARTY_ID has no duplicate value in the file

    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        assert len(fd) == 5_889_278

        t1 = time()
        index = FWFUniqueIndexDict(fwf)
        fwf_db_cython.create_index(fwf, "PARTY_ID", index)
        fwf_db_cython.create_index(fwf, "PARTY_ID", index)
        print("")
        print(f'1. Elapsed time is {time() - t1} seconds.    {len(index):,d}')
        assert len(fd) == len(index)

        # Unique index with plain dict
        # approx 20 seconds

        t1 = time()
        index = FWFIndexDict(fwf)
        fwf_db_cython.create_index(fwf, "PARTY_ID", index)
        fwf_db_cython.create_index(fwf, "PARTY_ID", index)
        print(f'2. Elapsed time is {time() - t1} seconds.    {len(index):,d}')
        assert len(fd) == len(index)

        # Non-unique index with FWFDict
        # approx 40 secs

        t1 = time()
        data = BytesDictWithIntListValues(len(fd) * 2)
        index = FWFIndexDict(fwf, data)
        fwf_db_cython.create_index(fwf, "PARTY_ID", index)
        fwf_db_cython.create_index(fwf, "PARTY_ID", index)
        print(f'3. Elapsed time is {time() - t1} seconds.    {len(index):,d}')
        assert len(fd) == len(index)

        # non-unique index with mem optimized dict
        # approx 53 secs. Quite a bit slower then defaultdict(list)


@pytest.mark.slow
def test_non_unique_index():
    # SALES_LOCATION_ID has several duplicate values

    fwf = FWFFile(CENT_SALES_ASSIGNMENT)
    with fwf.open(FILE_SALES_ASSIGNMENT) as fd:
        assert len(fd) == 1_036_3608

        t1 = time()
        index = FWFIndexDict(fwf)
        fwf_db_cython.create_index(fwf, "SALES_LOCATION_ID", index)
        print("")
        print(f'1. Elapsed time is {time() - t1} seconds.    {len(index):,d}')
        assert len(index) == 3_152_698

        # Non-unique index with FWFDict
        # approx 33 secs

        t1 = time()
        data = BytesDictWithIntListValues(len(fd) * 2)
        index = FWFIndexDict(fwf, data)
        fwf_db_cython.create_index(fwf, "SALES_LOCATION_ID", index)
        print(f'2. Elapsed time is {time() - t1} seconds.    {len(index):,d}')
        assert len(index) == 3_152_698

        # non-unique index with mem optimized dict
        # approx 40 secs. Quite a bit slower then defaultdict(list)

"""
# @pytest.mark.slow
# TODO This test is not yet implemented
def test_MyIndexDict_get():

    fwf = FWFFile(CENT_PARTY)
    with fwf.open(FILE_CENT_PARTY) as fd:
        rec_size = len(fd)
        assert rec_size == 5_889_278

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
"""

if __name__ == "__main__":
    #setup_module(None)

    test_cython_filter()
    #test_find_last()

    #teardown_module(None)
