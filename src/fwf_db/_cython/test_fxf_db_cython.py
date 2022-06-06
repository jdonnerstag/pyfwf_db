#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring

import pytest
import fwf_db
import fwf_db.fwf_cython
from . import fwf_db_cython

def test_say_hello():
    assert fwf_db_cython.say_hello_to("me") == "Hello me!"


class TestFile1:

    FIELDSPECS = [
        {"name": "any", "len": 1},
    ]

class TestFile2:

    FIELDSPECS = [
        {"name": "any", "len": 2},
    ]

class TestFile3:

    FIELDSPECS = [
        {"name": "any", "len": 999},
    ]


def exec_fwf_cython_empty(filedef, data):

    fwf = fwf_db.FWFFile(filedef)
    with fwf.open(data) as fd:
        assert len(fd) == 0

        rtn = fwf_db_cython.fwf_cython(fwf,
            -1, -1, -1, -1,
            None, None, None, None,
            index=None,
            unique_index=False,
            integer_index=False
        )
        # Both return the number of records in the file
        assert len(fd) == len(rtn)

def test_fwf_cython_empty():
    filedefs = [TestFile1, TestFile2, TestFile3]
    headers = [b"", b"#", b"# empty", b"# emtpy \n"]

    for filedef in filedefs:
        for header in headers:
            exec_fwf_cython_empty(filedef, header)

class TestFile4:

    FIELDSPECS = [
        {"name": "id", "len": 3},
    ]

class TestFile5:

    FIELDSPECS = [
        {"name": "id", "len": 3},
        {"name": "text", "len": 4},
    ]

def exec_get_field_data(filedef, data):
    fwf = fwf_db.FWFFile(filedef)
    with fwf.open(data) as fd:
        rtn = fwf_db_cython.get_field_data(fwf, "id")
        return rtn

def test_get_field_data():
    assert len(exec_get_field_data(TestFile4, b"")) == 0
    assert exec_get_field_data(TestFile4, b"000").tolist() == [b"000"]
    assert exec_get_field_data(TestFile4, b"000\n001").tolist() == [b"000", b"001"]
    assert exec_get_field_data(TestFile4, b"000\n001\n").tolist() == [b"000", b"001"]

    assert len(exec_get_field_data(TestFile5, b"")) == 0
    assert exec_get_field_data(TestFile5, b"000abcd").tolist() == [b"000"]
    assert exec_get_field_data(TestFile5, b"000abcd\n001abcd").tolist() == [b"000", b"001"]
    assert exec_get_field_data(TestFile5, b"000abcd\n001abcd\n").tolist() == [b"000", b"001"]

def exec_create_index(filedef, data):
    fwf = fwf_db.FWFFile(filedef)
    with fwf.open(data) as fd:
        rtn = fwf_db_cython.create_index(fwf, "id")
        return rtn

def test_create_index():
    assert exec_create_index(TestFile4, b"") == {}
    assert exec_create_index(TestFile4, b"000") == {b"000": [0]}
    assert exec_create_index(TestFile4, b"000\n001") == {b"000": [0], b"001": [1]}
    assert exec_create_index(TestFile4, b"000\n001\n") == {b"000": [0], b"001": [1]}
    assert exec_create_index(TestFile4, b"000\n001\n000") == {b"000": [0, 2], b"001": [1]}

def exec_create_unique_index(filedef, data):
    fwf = fwf_db.FWFFile(filedef)
    with fwf.open(data) as fd:
        rtn = fwf_db_cython.create_unique_index(fwf, "id")
        return rtn

def test_create_unqiue_index():
    assert exec_create_unique_index(TestFile4, b"") == {}
    assert exec_create_unique_index(TestFile4, b"000") == {b"000": 0}
    assert exec_create_unique_index(TestFile4, b"000\n001") == {b"000": 0, b"001": 1}
    assert exec_create_unique_index(TestFile4, b"000\n001\n") == {b"000": 0, b"001": 1}
    assert exec_create_unique_index(TestFile4, b"000\n001\n000") == {b"000": 2, b"001": 1}

def exec_get_int_field_data(filedef, data):
    fwf = fwf_db.FWFFile(filedef)
    with fwf.open(data) as fd:
        rtn = fwf_db_cython.get_int_field_data(fwf, "id")
        return rtn

def test_get_int_field_data():
    assert len(exec_get_int_field_data(TestFile4, b"")) == 0
    assert exec_get_int_field_data(TestFile4, b"000").tolist() == [0]
    assert exec_get_int_field_data(TestFile4, b"000\n001").tolist() == [0, 1]
    assert exec_get_int_field_data(TestFile4, b"000\n001\n").tolist() == [0, 1]

    assert len(exec_get_int_field_data(TestFile5, b"")) == 0
    assert exec_get_int_field_data(TestFile5, b"000abcd").tolist() == [0]
    assert exec_get_int_field_data(TestFile5, b"000abcd\n001abcd").tolist() == [0, 1]
    assert exec_get_int_field_data(TestFile5, b"000abcd\n001abcd\n").tolist() == [0, 1]

def exec_create_int_index(filedef, data):
    fwf = fwf_db.FWFFile(filedef)
    with fwf.open(data) as fd:
        rtn = fwf_db_cython.create_int_index(fwf, "id")
        return rtn

def test_create_int_index():
    assert exec_create_int_index(TestFile4, b"") == {}
    assert exec_create_int_index(TestFile4, b"000") == {0: [0]}
    assert exec_create_int_index(TestFile4, b"000\n001") == {0: [0], 1: [1]}
    assert exec_create_int_index(TestFile4, b"000\n001\n") == {0: [0], 1: [1]}
    assert exec_create_int_index(TestFile4, b"000\n001\n000") == {0: [0, 2], 1: [1]}

def exec_fwf_cython_filter(filedef, data, filters):

    fwf = fwf_db.FWFFile(filedef)
    with fwf.open(data) as fd:
        rtn = fwf_db_cython.fwf_cython(fwf,
            *filters,
            index=None,   # No index => return line numbers
            unique_index=False,
            integer_index=False
        )
        return rtn

def test_fwf_cython_filter():
    # Because no index creation is requested, all these tests return a list of line numbers
    no_filter = [-1, -1, -1, -1, None, None, None, None]
    assert len(exec_fwf_cython_filter(TestFile4, b"", no_filter)) == 0
    assert len(exec_fwf_cython_filter(TestFile4, b"\n", no_filter)) == 0
    assert len(exec_fwf_cython_filter(TestFile4, b"# ", no_filter)) == 0
    assert len(exec_fwf_cython_filter(TestFile4, b"# empty", no_filter)) == 0
    assert len(exec_fwf_cython_filter(TestFile4, b"# line \n# empty", no_filter)) == 0

    assert exec_fwf_cython_filter(TestFile4, b"# comment\n333\n444", no_filter).tolist() == [0, 1]
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", no_filter).tolist() == [0, 1, 2, 3]

    with pytest.raises(Exception):
        # Whenever pos != -1, a value must be provided
        assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [0, -1, -1, -1, None, None, None, None]).tolist() == [0, 1, 2, 3]

    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [0, 0, -1, -1, b"000", b"999", None, None]).tolist() == [0, 1, 2, 3]
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [-1, -1, 0, 0, None, None, b"000", b"999"]).tolist() == [0, 1, 2, 3]
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [0, 0, 0, 0, b"000", b"999", b"000", b"999"]).tolist() == [0, 1, 2, 3]

    # The lower bound is inclusive. The upper bound is exclusive.
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [0, 0, -1, -1, b"111", b"444", None, None]).tolist() == [0, 1, 2]
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [-1, -1, 0, 0, None, None, b"111", b"444"]).tolist() == [0, 1, 2]

    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [0, 0, -1, -1, b"112", b"444", None, None]).tolist() == [1, 2]
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [-1, -1, 0, 0, None, None, b"112", b"444"]).tolist() == [1, 2]

    # An empty value equals lowest or highest possible value
    assert exec_fwf_cython_filter(TestFile4, b"111\n   \n333\n444", [0, 0, -1, -1, b"111", b"444", None, None]).tolist() == [0, 1, 2]
    assert exec_fwf_cython_filter(TestFile4, b"111\n   \n333\n444", [-1, -1, 0, 0, None, None, b"111", b"444"]).tolist() == [0, 1, 2]

    # Only filter on lower bound
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [0, -1, -1, -1, b"222", None, None, None]).tolist() == [1, 2, 3]
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [-1, -1, 0, -1, None, None, b"222", None]).tolist() == [1, 2, 3]
    assert exec_fwf_cython_filter(TestFile4, b"111\n   \n333\n444", [0, -1, -1, -1, b"222", None, None, None]).tolist() == [1, 2, 3]
    assert exec_fwf_cython_filter(TestFile4, b"111\n   \n333\n444", [-1, -1, 0, -1, None, None, b"222", None]).tolist() == [1, 2, 3]

    # Only filter on upper bound
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [-1, 0, -1, -1, None, b"444", None, None]).tolist() == [0, 1, 2]
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [-1, -1, -1, 0, None, None, None, b"444"]).tolist() == [0, 1, 2]
    assert exec_fwf_cython_filter(TestFile4, b"111\n   \n333\n444", [-1, 0, -1, -1, None, b"444", None, None]).tolist() == [0, 1, 2]
    assert exec_fwf_cython_filter(TestFile4, b"111\n   \n333\n444", [-1, -1, -1, 0, None, None, None, b"444"]).tolist() == [0, 1, 2]

    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [0, -1, -1, 0, b"222", None, None, b"444"]).tolist() == [1, 2]
    assert exec_fwf_cython_filter(TestFile4, b"111\n222\n333\n444", [-1, 0, 0, -1, None, b"444", b"222", None]).tolist() == [1, 2]
