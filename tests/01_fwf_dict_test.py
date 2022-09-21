#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name

from fwf_db import FWFDict


def test_fwf_dict_setitem():
    d = FWFDict()
    assert len(d) == 0

    d[1] = 111
    assert len(d) == 1
    assert d[1] == [111]

    d[1] = 122
    assert len(d) == 1
    assert d[1] == [111, 122]

    d[1] = 133
    assert len(d) == 1
    assert d[1] == [111, 122, 133]

    d[2] = 222
    assert len(d) == 2
    assert d[2] == [222]

    d.set(3, 333)
    assert len(d) == 3
    assert d[3] == [333]


def test_fwf_dict_update():
    d = FWFDict()
    d.update([(1, 111), (2, 222), (1, 122), (1, 133)])
    assert len(d) == 2
    assert d[1] == [111, 122, 133]
    assert d[2] == [222]
