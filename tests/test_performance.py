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
from fwf_db.fwf_unique_np_based import FWFUniqueNpBased
from fwf_db.fwf_index_np_based import FWFIndexNumpyBased
from fwf_db.fwf_cython_unique_index import FWFCythonUniqueIndex
from fwf_db.fwf_operator import FWFOperator as op

from fwf_db.cython import fwf_db_ext

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


DIR = "../PIL-data-2/11_input_crlf_fixed/"
FILE_1 = DIR + "ANO_DWH..DWH_TO_PIL_CENT_PARTY_VTAG.20180119104659.A901"

@pytest.mark.slow
def test_perf_iter_lines():
    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
        assert len(fd) == 5889278   # The file is 2GB and has 5.8 mio records

        t1 = time()
        for i, line in fd.iter_lines():
            assert i >= 0
            assert line

        print(f'Elapsed time is {time() - t1} seconds.')

        # In run mode: Just read the lines (bytes)
        # 7.170794725418091     # And my SDD shows almost no signs of being busy => CPU contraint


@pytest.mark.slow
def test_perf_iter_fwfline():
    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
        assert len(fd) == 5889278

        t1 = time()
        for line in fd:
            assert line

        print(f'Elapsed time is {time() - t1} seconds.')

        # In run mode: wrap the line into a FWFLine object
        # 12.35709834098816     # 5-6 secs more (almost doubled the time)


@pytest.mark.slow
def test_perf_simple_index():
    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
        assert len(fd) == 5889278   
        t1 = time()
        index = FWFSimpleIndex(fd).index("PARTY_ID")  
        print(f'Elapsed time is {time() - t1} seconds.')
        assert len(index) == 5889278    # No duplictates, which makes sense for this data

        idx = list(index.data.keys())
        t1 = time()
        for _ in range(int(1e6)):
            key =  randrange(len(index))
            key = idx[key]
            refs = index[key]
            assert refs

        print(f'Elapsed time is {time() - t1} seconds.')

        # run mode: Create an index on PARTY_ID, using the optimized 
        # field reader (no FWFLine object)
        # Elapsed time is 22.193581104278564 seconds.   # 10 - 15 secs to create the index
        # 
        # and access lines randomly 1 mio times
        # Elapsed time is 6.269562721252441 seconds.


@pytest.mark.slow
def test_numpy_samples():
    reclen = int(10e6)
    values = np.empty(reclen, dtype="S10")

    t1 = time()
    flen = int(reclen / 15)
    for i in range(reclen):
        value = randrange(flen)
        value = f"{value:>10}"
        value = bytes(f"{value:>10}", "utf-8")
        values[i] = value

    print(f'Elapsed time is {time() - t1} seconds.')

    t1 = time()
    data = defaultdict(list)
    all(data[value].append(i) or True for i, value in enumerate(values))
    print(f'Elapsed time is {time() - t1} seconds.')

    t1 = time()
    for i in range(int(1e6)):
        value = randrange(flen)
        value = f"{value:>10}"
        value = bytes(f"{value:>10}", "utf-8")

        rec = data.get(value, None)
        assert rec is not None

    print(f'Elapsed time is {time() - t1} seconds.')


@pytest.mark.slow
def test_perf_numpy_index():

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
        assert len(fd) == 5889278   
        t1 = time()
        index = FWFIndexNumpyBased(fd).index("PARTY_ID")  
        print(f'Elapsed time is {time() - t1} seconds.')
        assert len(index) == 5889278    # No duplictates, which makes sense for this data

        idx = list(index.keys())
        t1 = time()
        for _ in range(int(1e6)):
            key =  randrange(len(index))
            key = idx[key]
            refs = index[key]
            assert refs

        print(f'Elapsed time is {time() - t1} seconds.')

        # In run mode it takes: Create an index on PARTY_ID, using the optimized 
        # field reader (no FWFLine object)
        # Elapsed time is 20.20484495162964 seconds.   # a tiny bit faster then simple index
        # 
        # and access lines randomly 1 mio times
        # Elapsed time is 5.6988043785095215 seconds.
        # That is pretty much the same result, that the simple python based index provides.
        # Which makes sense, as both create dicts


@pytest.mark.slow
def test_effective_date_simple_filter():

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
        assert len(fd) == 5889278   

        t1 = time()
        fd = fd.filter(op("BUSINESS_DATE") < b"20180118")
        # They are dummy data with all the same change date !?!?
        assert len(fd) == 0

        print(f'Elapsed time is {time() - t1} seconds.')

        # In run mode: 
        # 21.847846508026123     # Compared to 7 secs for simple bytes and 12 secs for FWFLines


@pytest.mark.slow
def test_effective_date_region_filter():

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
        assert len(fd) == 5889278   

        t1 = time()
        # NOTE: you can not combine the operators with and resp. or
        fd = fd.filter(op("VALID_FROM") <= b"20130101")
        fd = fd.filter(op("VALID_UNTIL") >= b"20131231")
        assert len(fd) == 1293435

        print(f'Elapsed time is {time() - t1} seconds.')

        # In run mode: 
        # 29.993732929229736    # Compared to 22 secs for only 1 filter


@pytest.mark.slow
def test_effective_date_region_filter_optmized():

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
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
        assert len(fd) == 1293435

        print(f'Elapsed time is {time() - t1} seconds.')

        # In run mode: 
        # 18.807480335235596    # Faster then then FWFOperator !!
        # But still CPU bound.


@pytest.mark.slow
def test_cython_like_filter():
    """Still a python impl. but the interface is close"""

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
        assert len(fd) == 5889278   

        region_filter = fwf.iter_and_filter(
            fd.fields["BUSINESS_DATE"].start, b"20180120",
            -1, None, 
            fd.fields["VALID_FROM"].start, b"20130101",
            fd.fields["VALID_UNTIL"].start, b"20131231",
        )

        t1 = time()
        fd = fd.filter(region_filter)
        assert len(fd) == 1293435

        print(f'Elapsed time is {time() - t1} seconds.')

        # In run mode: 
        # 29.85299563407898    # Rather slow. It shows that every single python
        #                      # instructions adds on top.

@pytest.mark.slow
def test_cython_filter_ex():

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
        assert len(fd) == 5889278   

        t1 = time()
        rtn = fwf_db_ext.iter_and_filter(fwf,
            fd.fields["BUSINESS_DATE"].start, b"20180120",
            -1, None, 
            fd.fields["VALID_FROM"].start, b"20130101",
            fd.fields["VALID_UNTIL"].start, b"20131231",
        )

        rlen = rtn.buffer_info()[1]
        assert rlen == 1293435
        print(f'Elapsed time is {time() - t1} seconds.    {rlen}')

        # In run mode: 
        # 2.1357336044311523   # Yes !!!!


@pytest.mark.slow
def test_cython_get_field_data():

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
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

    with fwf.open(FILE_1) as fd:
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

    with fwf.open(FILE_1) as fd:
        assert len(fd) == 5889278   

        t1 = time()
        rtn = fwf_db_ext.get_field_data(fwf, "PARTY_ID")
        print(f'Elapsed time is {time() - t1} seconds.')

        #rtn = np.argsort(rtn)
        rtn = np.argsort(rtn)
        print(f'Elapsed time is {time() - t1} seconds. {len(rtn):,d}')

        # In run mode: 
        # 4.589160680770874   # just 2.2 secs to sort


#@pytest.mark.slow
def test_cython_create_index():

    fwf_db_ext.say_hello_to("Susie")

    fwf = FWFFile(CENT_PARTY)

    with fwf.open(FILE_1) as fd:
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

        """ """
        t1 = time()
        rtn = fwf_db_ext.get_field_data(fwf, "PARTY_ID")
        print(f'Elapsed time is {time() - t1} seconds.')

        rtn_sorted = np.argsort(rtn)
        print(f'Elapsed time is {time() - t1} seconds.')

        result = dict()
        h1 = None
        ar = None
        for i in rtn_sorted:
            h2 = rtn[i]
            if h1 != h2:
                h1 = h2
                ar = [i]
                result[h1] = ar
            else:
                ar.append(i)

        print(f'Elapsed time is {time() - t1} seconds.')

        # 4.449473857879639 sec to prepare
        # 28.354421854019165 for 1 mio searches (compared ro 3 secs with a dict)
        """ """


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    # test_perf_iter_lines()
    # test_perf_iter_fwfline()
    # test_perf_simple_index()
    # test_perf_numpy_index()
    # test_numpy_samples()
    # test_effective_date_simple_filter()
    # test_effective_date_region_filter()
    # test_effective_date_region_filter_optmized()
    # test_cython_like_filter()
    # test_cython_filter_ex()
    test_cython_create_index()
    # test_cython_get_field_data()
    # test_find_last()
    # test_numpy_sort()
