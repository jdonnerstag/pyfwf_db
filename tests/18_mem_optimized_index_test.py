#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name, protected-access

from random import randrange
from time import time

import pytest

from fwf_db._cython.fwf_mem_optimized_index import BytesDictWithIntListValues


def test_constructor():

    data = BytesDictWithIntListValues(1000)
    assert data is not None


def test_add():

    data = BytesDictWithIntListValues(1000)
    assert len(data) == 0

    data["111"] = 1
    data["222"] = 2
    data["111"] = 11
    data["111"] = 111

    assert len(data) == 2
    assert "111" in data
    assert "222" in data


def test_get():

    data = BytesDictWithIntListValues(1000)
    with pytest.raises(Exception):
        data["xxx"]     # pylint: disable=pointless-statement

    data["111"] = 1
    assert data["111"] == [1]

    data["111"] = 11
    assert data["111"] == [1, 11]
    assert data.get("111") == [1, 11]


def test_large():

    data = BytesDictWithIntListValues(int(10e6))
    with pytest.raises(Exception):
        data["xxx"]     # pylint: disable=pointless-statement

    data["111"] = 1
    assert data["111"] == [1]

    data["111"] = 11
    assert data["111"] == [1, 11]
    assert data.get("111") == [1, 11]

    # Fill the array with 1 million random numbers
    t1 = time()
    for i in range(int(1e6)):
        key = randrange(int(1e6))
        data[key] = i

    print(f'Fill array: Elapsed time is {time() - t1} seconds. Added {len(data):,d} keys')

    # Access the array
    t1 = time()
    for i in range(int(1e6)):
        data.get(i)     # pylint: disable=pointless-statement

    print(f'Access array: Elapsed time is {time() - t1} seconds.')

    t1 = time()
    data.finish()
    print(f'Finish array: Elapsed time is {time() - t1} seconds.')

    # Access the array
    t1 = time()
    for i in range(int(1e6)):
        data.get(i)     # pylint: disable=pointless-statement

    print(f'Access 2 array: Elapsed time is {time() - t1} seconds.')
