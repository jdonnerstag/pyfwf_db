#!/usr/bin/env python
# encoding: utf-8

import pytest

import os
import sys
import io
from random import randrange
from time import time
import numpy as np
import pandas as pd
from collections import defaultdict

from fwf_db import FWFFile, FWFSimpleIndex, FWFMultiFile, FWFUnique
from fwf_db.fwf_unique_np_based import FWFUniqueNpBased
from fwf_db.fwf_index_np_based import FWFIndexNumpyBased
from fwf_db.fwf_operator import FWFOperator as op


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

        # In run mode it takes: Create an index on PARTY_ID, using the optimized 
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

        # This is really slow
        t1 = time()
        #idx = list(index.data.groups.keys())
        #idx = list(index.data.groups)
        df = index.data.tail(1)
        df = df["values"]
        print(f'Elapsed time is {time() - t1} seconds.')

        t1 = time()
        for _ in range(int(1e6)):
            key =  randrange(len(index))
            key = df.iloc[key]
            refs = index[key]
            assert refs

        print(f'Elapsed time is {time() - t1} seconds.')

        # In run mode it takes: Create an index on PARTY_ID, using the optimized 
        # field reader (no FWFLine object)
        # Elapsed time is 6.427615165710449 seconds.   # as fast as reading line by line
        # 
        # and access lines randomly 1 mio times
        # Elapsed time is 6.269562721252441 seconds.

"""
@pytest.mark.slow
def test_effective_date():
    fwf1 = FWFFile(DataFile)
    fwf2 = FWFFile(DataFile)

    with fwf1.open(DATA) as fd1, fwf2.open(DATA) as fd2:

        mf = FWFMultiFile()
        mf.add_file(fd1)
        mf.add_file(fd2)

        assert len(mf) == 20
        assert len(mf.files) == 2
        assert mf.lines == slice(0, 20)

        # Filter all records by effective date
        filtered = mf.filter(op("changed") <= b" 20180501")
        for i, line in filtered.iter_lines():
            line = line.decode("utf-8")
            x = int(line[0:5])
            assert 1 <= x <= 5

        # And now create an index
        index = FWFSimpleIndex(filtered).index("ID")
        assert len(index) == 5
        for key in index:
            refs = index[key]
            assert len(refs) == 2

            line = refs[0]
            line = int(line["ID"].decode("utf-8"))
            assert 1 <= x <= 5
            
            line = refs[1]
            line = int(line["ID"].decode("utf-8"))
            assert 1 <= x <= 5

        # TODO May it would be helpful to use parent consistently instead of fwfview and fwffile
        # TODO delevel is currently only available in SimpleIndex and not yet very flexible
        index = index.delevel()
        assert len(index) == 5
        for key in index:
            refs = index[key]
            assert len(refs) == 2

            line = refs[0]
            assert (0 <= line.lineno < 5) or (10 <= line.lineno < 15)
            
            line = refs[1]
            assert (0 <= line.lineno < 5) or (10 <= line.lineno < 15)
"""

# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    # test_perf_iter_lines()
    # test_perf_iter_fwfline()
    # test_perf_numpy_index()
    test_numpy_samples()