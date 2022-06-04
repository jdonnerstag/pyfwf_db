#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring

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
            None, None, None,
            None, None, None,
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

def exec_fwf_cython(filedef, data):

    fwf = fwf_db.FWFFile(filedef)
    with fwf.open(data) as fd:
        assert len(fd) == 0

        rtn = fwf_db_cython.fwf_cython(fwf,
            "id", None, None,
            None, None, None,
            index=None,
            unique_index=False,
            integer_index=False
        )
        # Both return the number of records in the file
        assert len(fd) == len(rtn)
