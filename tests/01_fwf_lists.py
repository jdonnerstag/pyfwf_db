#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name

import pytest

from fwf_db.fwf_lists import FWFList


def test_fwf_list_getitem():
    data = FWFList(8, 10)

    # Access all elements and validate all entry are zeroed
    for i in range(10):
        assert len(data[i]) == 8
        assert data[i] == b"\0\0\0\0\0\0\0\0"

    with pytest.raises(IndexError):
        _ = data[10]


def test_fwf_list_setitem():
    data = FWFList(8, 10)
    for i in range(10):
        data[i] = bytes([i]) * 8

    for i in range(10):
        assert len(data[i]) == 8
        for x in data[i]:
            assert x == i


    # It is possible to set values of any length
    data = FWFList(8, 10)
    for i in range(0, 10, 2):
        data[i] = bytes([i + 10]) * 16

    for i in range(10):
        assert len(data[i]) == 8
        for x in data[i]:
            assert x == int((i + 10) / 2) * 2


    data = FWFList(8, 10)
    for i in range(0, 10):
        data[i] = bytes([i + 20]) * 4

    for i in range(10):
        assert len(data[i]) == 8
        data[i] = bytes([i]) * 4 + bytes(0) * 4


    data = FWFList(8, 10)
    with pytest.raises(ValueError):
        data[9] = bytes([1]) * 9

    with pytest.raises(ValueError):
        data[9] = bytes([1]) * (8 + 1)

    with pytest.raises(ValueError):
        data[8] = bytes([1]) * (8 + 8 + 1)

    # It is possible to create a 0 byte array
    data = FWFList(8, 0)
    with pytest.raises(IndexError):
        data[0] = bytes([1]) * 8


def test_fwf_list_append():
    data = FWFList(8, 10)
    assert len(data) == 0

    # Setting a value, updates the count.
    data[0] = bytes([1]) * 8
    assert len(data) == 1

    data.append(bytes([1]) * 8)
    assert len(data) == 2
    assert data[1] == bytes([1]) * 8

    data.extend([bytes([2]) * 8] * 2)
    assert len(data) == 4
    assert data[0] == bytes([1]) * 8
    assert data[1] == bytes([1]) * 8
    assert data[2] == bytes([2]) * 8
    assert data[3] == bytes([2]) * 8

    for i, value in enumerate(data):
        if i in [0, 1]:
            assert value == bytes([1]) * 8
        else:
            assert value == bytes([2]) * 8


def test_fwf_list_contains():
    data = FWFList(8, 10)
    for i in range(10):
        data[i] = bytes([i]) * 8


    for i in range(10):
        x = bytes([i]) * 8
        assert data.find(x) == i
        assert x in data

        x = bytes([i]) * 4
        assert data.find(x) == i
        assert x in data

    x = bytes([1]) * 8
    x += bytes([2]) * 8
    assert data.find(x) == 1
    assert x in data
