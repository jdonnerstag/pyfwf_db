#!/usr/bin/env python
# encoding: utf-8

import pytest

import abc
import os
import sys
import io
from random import randrange
from time import time

from fwf_db.fwf_mem_optimized_index import BytesDictWithIntListValues, MyIndexDict


def test_constructor():

    data = BytesDictWithIntListValues(1000)
    assert data is not None


def test_add():

    data = BytesDictWithIntListValues(1000)
    assert len(data) == 0

    data["111"] = 1
    data["222"] = (2, 22)
    data["111"] = 11
    data["111"] = 111

    assert len(data) == 2
    assert "111" in data
    assert "222" in data


def test_get():

    data = BytesDictWithIntListValues(1000)
    with pytest.raises(Exception):
        data["xxx"]

    data["111"] = 1
    assert data["111"] == [(0, 1)]

    data["111"] = 11
    assert data["111"] == [(0, 1), (0, 11)]
    assert data.get("111") == [(0, 1), (0, 11)]



def test_large():

    data = BytesDictWithIntListValues(int(10e6))
    with pytest.raises(Exception):
        data["xxx"]

    data["111"] = 1
    assert data["111"] == [(0, 1)]

    data["111"] = 11
    assert data["111"] == [(0, 1), (0, 11)]
    assert data.get("111") == [(0, 1), (0, 11)]

    t1 = time()
    for i in range(int(1e6)):
        key = randrange(int(1e6))
        data[key] = i

    print(f'Elapsed time is {time() - t1} seconds.    {len(data):,d}')

    t1 = time()
    for i in range(int(1e6)):
        data[key]

    print(f'Elapsed time is {time() - t1} seconds.')


def test_resize():

    data = BytesDictWithIntListValues(0)
    with pytest.raises(Exception):
        data["1"] = 1

    data = data.resize(10)
    data["1"] = 1
    assert len(data.next) == 12  # 2 used plus 10

    data = data.resize(10)
    data["1"] = 1
    assert len(data.next) == 13  # 3 used plus 10


def test_MyIndexDict_constructor():
    data = MyIndexDict(size=100, mm=" " * 100, reclen=20, field_pos=0, field_len=1, align="left")
    assert data


def test_MyIndexDict_hash():
    data = MyIndexDict(size=100, mm=b" " * 100, reclen=20, field_pos=0, field_len=1, align="left")
    assert data

    assert data.hash(b"1") == 356613032893705355295
    assert data.hash(b"1234567") == 358040167757824373535

    data = MyIndexDict(size=100, mm=b" " * 100, reclen=20, field_pos=0, field_len=2, align="left")
    assert data

    assert data.hash(b" 1") == 234283163590324125727
    assert data.hash(b"1 ") == 357522760018434195487

    assert data.hash(b"12345678") == 358040167757824379191

    with pytest.raises(Exception):
        assert data.hash(b"123456789")


def test_MyIndexDict_put():
    data = MyIndexDict(size=100, mm=b" " * 100, reclen=20, field_pos=0, field_len=1, align="left")
    assert data

    data.put(b"1 34567890", 1)
    assert data._last == 1
    assert data._start[10] == 1    # the hash code of "1" evaluates to bucket 10
    assert data._lineno[1] == 1    # first k/v => first index (0 is reserved for end-of-list)

    data.put(b"2", 2)
    assert data._last == 2
    assert data._start[15] == 2    # the hash code of "2" evaluates to bucket 15
    assert data._lineno[2] == 2    # first k/v => first index (0 is reserved for end-of-list)


    data = MyIndexDict(size=100, mm=b" " * 100, reclen=20, field_pos=0, field_len=3, align="left")
    for i in range(100):
        key = bytes(str(i), "utf-8")
        data.put(key, i)

    percentage_filled, buckets_by_length, max_length, buckets_length = data.analyze()
    print(f"precentage filled: {percentage_filled}, max_length: {max_length}, distibution: {buckets_by_length}")

    count = 0
    for k, v in buckets_by_length.items():
        count += int(k) * int(v)
    assert count == 100

    # This test gives a pretty bad result: 40% utilisation and max-depth of 6
    data = MyIndexDict(size=100000, mm=b" " * 10000, reclen=20, field_pos=0, field_len=6, align="left")
    for i in range(100000):
        key = bytes(str(i), "utf-8")
        data.put(key, i)

    percentage_filled, buckets_by_length, max_length, buckets_length = data.analyze()
    print(f"precentage filled: {percentage_filled}, max_length: {max_length}, distibution: {buckets_by_length}")

    # Better: 60% utilisation and max-depth 4
    data = MyIndexDict(size=100111, mm=b" " * 10000, reclen=20, field_pos=0, field_len=6, align="left")
    for i in range(100000):
        key = bytes(str(i), "utf-8")
        data.put(key, i)

    percentage_filled, buckets_by_length, max_length, buckets_length = data.analyze()
    print(f"precentage filled: {percentage_filled}, max_length: {max_length}, distibution: {buckets_by_length}")


    # Bad: 39% utilisation and max-depth 5.  Simply increasing size doesn't solve the issue
    data = MyIndexDict(size=110111, mm=b" " * 10000, reclen=20, field_pos=0, field_len=6, align="left")
    for i in range(100000):
        key = bytes(str(i), "utf-8")
        data.put(key, i)

    percentage_filled, buckets_by_length, max_length, buckets_length = data.analyze()
    print(f"precentage filled: {percentage_filled}, max_length: {max_length}, distibution: {buckets_by_length}")


    # Bad: Not getting better without shuffling the initial hash value
    data = MyIndexDict(size=110111, mm=b" " * 10000, reclen=20, field_pos=0, field_len=6, align="left")
    data.hash_calc = lambda data: data

    for i in range(100000):
        key = bytes(str(i), "utf-8")
        data.put(key, i)

    percentage_filled, buckets_by_length, max_length, buckets_length = data.analyze()
    print(f"precentage filled: {percentage_filled}, max_length: {max_length}, distibution: {buckets_by_length}")


    # 58% utilisation and max-depth 5.
    data = MyIndexDict(size=1000000, mm=b" " * 10000, reclen=20, field_pos=0, field_len=7, align="left")
    for i in range(1000000):
        key = bytes(str(i), "utf-8")
        data.put(key, i)

    percentage_filled, buckets_by_length, max_length, buckets_length = data.analyze()
    print(f"precentage filled: {percentage_filled}, max_length: {max_length}, distibution: {buckets_by_length}")

    # TODO another idea for an index
    # TODO Our CSV is ASCII, hence many byte combinations will never occur
    # TODO Lets assume we use 256 entry (1 byte) lookup tables, for every byte found.
    # TODO for an 10 byte index, 10 consequtive lookups would be needed to find an exact match
    # TODO Given the test above, the hash approach also requires 4-6 hops given our current hash implementation
    

def test_MyIndexDict_get():
    data = MyIndexDict(size=100, mm=b"1234567890\nabcdefghij\nklmnopqrst\nuvwxyzABCD" * 100, reclen=11, field_pos=0, field_len=1, align="left")
    assert data

    data.put(b"a", 1)
    assert data.get(b"a") == 1

    assert data.get(b"#") == None

    data._put_into_bucket(2, 0)
    data._put_into_bucket(2, 1)
    data._put_into_bucket(2, 2)
    data._put_into_bucket(2, 3)

    assert data._get_from_bucket(1, b"1") == None
    assert data._get_from_bucket(117, b"1") == None
    
    assert data._get_from_bucket(2, b"1") == 0
    assert data._get_from_bucket(2, b"a") == 1
    assert data._get_from_bucket(2, b"k") == 2
    assert data._get_from_bucket(2, b"u") == 3
    assert data._get_from_bucket(2, b"#") == None


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_constructor()
