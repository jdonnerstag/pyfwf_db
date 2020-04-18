#!/usr/bin/env python
# encoding: utf-8

import pytest

import abc
import os
import sys
import io
from random import randrange
from time import time

from fwf_db.fwf_mem_optimized_index import BytesDictWithIntListValues


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


# Note: On Windows all of your multiprocessing-using code must be guarded by if __name__ == "__main__":
if __name__ == '__main__':

    test_constructor()
