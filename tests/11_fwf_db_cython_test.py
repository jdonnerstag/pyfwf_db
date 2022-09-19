#!/usr/bin/env python
# encoding: utf-8

'''Test all functionalities externalized into the Cython module

The Cython module is a low level module and protected (./_cython).
It should not be necessary by user code to use it directly.
'''

# pylint: disable=missing-class-docstring, missing-function-docstring, invalid-name

from fwf_db import FWFFile
from fwf_db._cython import fwf_db_cython
from fwf_db.fwf_index_like import FWFIndexDict, FWFUniqueIndexDict


def test_say_hello():
    """ Make sure we can load the lib and invoke same basic function """
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
    fwf = FWFFile(filedef)
    with fwf.open(data) as fd:
        assert len(fd) == 0

        db = fwf_db_cython.line_numbers(fwf)
        # Both return the number of records in the file
        assert len(fd) == len(db)


def test_fwf_cython_empty():
    filedefs = [TestFile1, TestFile2, TestFile3]
    headers = [b"", b"#", b"# empty", b"# empty \n", b"\n", b"# ", b"# line \n# empty"]

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

def init_filters(fwf, filters):
    filter_args = None
    if filters:
        filter_args = fwf_db_cython.FWFFilters(fwf)
        for f in filters:
            f += [None, None]
            filter_args.add_filter(f[0], f[1], f[2])

    return filter_args


def exec_line_number(filedef, data, filters=None):
    fwf = FWFFile(filedef)
    with fwf.open(data):
        filter_args = init_filters(fwf, filters)
        db = fwf_db_cython.line_numbers(fwf, filters=filter_args)
        return db.tolist()


def test_line_number():
    assert len(exec_line_number(TestFile4, b"")) == 0
    assert len(exec_line_number(TestFile4, b"#")) == 0
    assert len(exec_line_number(TestFile4, b"# comment \n# line")) == 0

    assert exec_line_number(TestFile4, b"000") == [0]
    assert exec_line_number(TestFile4, b"# \n000") == [0]
    assert exec_line_number(TestFile4, b"# comment \n# line \n000") == [0]

    assert exec_line_number(TestFile4, b"000\n001") == [0, 1]
    assert exec_line_number(TestFile4, b"000\n001\n") == [0, 1]

    assert len(exec_line_number(TestFile5, b"")) == 0
    assert exec_line_number(TestFile5, b"000abcd") == [0]
    assert exec_line_number(TestFile5, b"000abcd\n001abcd") == [0, 1]
    assert exec_line_number(TestFile5, b"000abcd\n001abcd\n") == [0, 1]

    assert exec_line_number(TestFile4, b"111\n222\n333\n444", [["id", b"000", b"999"]]) == [0, 1, 2, 3]
    assert exec_line_number(TestFile4, b"111\n222\n333\n444", [["id", b"000", b"999"], ["id", b"000", b"999"]]) == [0, 1, 2, 3]

    # The lower bound is inclusive. The upper bound is exclusive.
    assert exec_line_number(TestFile4, b"111\n222\n333\n444", [["id", b"000", b"444"]]) == [0, 1, 2]
    assert exec_line_number(TestFile4, b"111\n222\n333\n444", [["id", b"112", b"444"]]) == [1, 2]

    # An empty value equals lowest or highest possible value
    assert exec_line_number(TestFile4, b"111\n   \n333\n444", [["id", b"111", b"444"]]) == [0, 1, 2]

    # Only filter on lower bound
    assert exec_line_number(TestFile4, b"111\n222\n333\n444", [["id", b"222"]]) == [1, 2, 3]
    assert exec_line_number(TestFile4, b"111\n   \n333\n444", [["id", b"222"]]) == [1, 2, 3]

    # Only filter on upper bound
    assert exec_line_number(TestFile4, b"111\n222\n333\n444", [["id", None, b"444"]]) == [0, 1, 2]
    assert exec_line_number(TestFile4, b"111\n222\n333\n444", [["id", b"000", b"444"]]) == [0, 1, 2]
    assert exec_line_number(TestFile4, b"111\n   \n333\n444", [["id", b"000", b"444"]]) == [0, 1, 2]
    assert exec_line_number(TestFile4, b"111\n   \n333\n444", [["id", b"000", b"444"]]) == [0, 1, 2]

    assert exec_line_number(TestFile4, b"111\n222\n333\n444", [["id", b"222"], ["id", None, b"444"]]) == [1, 2]
    assert exec_line_number(TestFile4, b"111\n222\n333\n444", [["id", None, b"444"], ["id", b"222"]]) == [1, 2]


def exec_get_field_data(filedef, data):
    fwf = FWFFile(filedef)
    with fwf.open(data):
        db = fwf_db_cython.field_data(fwf, "id")
        return db.tolist()


# TODO We need to add test with filters
def test_get_field_data():
    assert len(exec_get_field_data(TestFile4, b"")) == 0
    assert exec_get_field_data(TestFile4, b"000") == [b"000"]
    assert exec_get_field_data(TestFile4, b"000\n001") == [b"000", b"001"]
    assert exec_get_field_data(TestFile4, b"000\n001\n")== [b"000", b"001"]

    assert len(exec_get_field_data(TestFile5, b"")) == 0
    assert exec_get_field_data(TestFile5, b"000abcd") == [b"000"]
    assert exec_get_field_data(TestFile5, b"000abcd\n001abcd") == [b"000", b"001"]
    assert exec_get_field_data(TestFile5, b"000abcd\n001abcd\n") == [b"000", b"001"]


def exec_get_int_field_data(filedef, data):
    fwf = FWFFile(filedef)
    with fwf.open(data):
        db = fwf_db_cython.field_data(fwf, "id", int_value=True)
        return db.tolist()


def test_get_int_field_data():
    assert len(exec_get_int_field_data(TestFile4, b"")) == 0
    assert exec_get_int_field_data(TestFile4, b"000") == [0]
    assert exec_get_int_field_data(TestFile4, b"000\n001") == [0, 1]
    assert exec_get_int_field_data(TestFile4, b"000\n001\n") == [0, 1]

    assert len(exec_get_int_field_data(TestFile5, b"")) == 0
    assert exec_get_int_field_data(TestFile5, b"000abcd") == [0]
    assert exec_get_int_field_data(TestFile5, b"000abcd\n001abcd") == [0, 1]
    assert exec_get_int_field_data(TestFile5, b"000abcd\n001abcd\n") == [0, 1]


def exec_create_index(filedef, data, func=None):
    fwf = FWFFile(filedef)
    index = FWFIndexDict(fwf)
    with fwf.open(data):
        fwf_db_cython.create_index(fwf, "id", index, func=func)
        return index.data

def test_create_index():
    assert not exec_create_index(TestFile4, b"")
    assert exec_create_index(TestFile4, b"000\n001") == {b"000": [0], b"001": [1]}
    assert exec_create_index(TestFile4, b"000") == {b"000": [0]}
    assert exec_create_index(TestFile4, b"000\n001") == {b"000": [0], b"001": [1]}
    assert exec_create_index(TestFile4, b"000\n001\n") == {b"000": [0], b"001": [1]}
    assert exec_create_index(TestFile4, b"000\n001\n000") == {b"000": [0, 2], b"001": [1]}

    assert not exec_create_index(TestFile4, b"", lambda x: int(x, base=10))
    assert exec_create_index(TestFile4, b"000\n001\n000", lambda x: int(x, base=10)) == {0: [0, 2], 1: [1]}


def exec_create_unique_index(filedef, data):
    fwf = FWFFile(filedef)
    index = FWFUniqueIndexDict(fwf)
    with fwf.open(data):
        fwf_db_cython.create_index(fwf, "id", index)
        return index.data

def test_create_unique_index():
    assert not exec_create_unique_index(TestFile4, b"")
    assert exec_create_unique_index(TestFile4, b"000") == {b"000": 0}
    assert exec_create_unique_index(TestFile4, b"000\n001") == {b"000": 0, b"001": 1}
    assert exec_create_unique_index(TestFile4, b"000\n001\n") == {b"000": 0, b"001": 1}
    assert exec_create_unique_index(TestFile4, b"000\n001\n000") == {b"000": 2, b"001": 1}


def exec_create_int_index(filedef, data):
    fwf = FWFFile(filedef)
    index = FWFIndexDict(fwf)
    with fwf.open(data):
        fwf_db_cython.create_index(fwf, "id", index, func="int")
        return index.data


def test_create_int_index():
    assert not exec_create_int_index(TestFile4, b"")
    assert exec_create_int_index(TestFile4, b"000") == {0: [0]}
    assert exec_create_int_index(TestFile4, b"000\n001") == {0: [0], 1: [1]}
    assert exec_create_int_index(TestFile4, b"000\n001\n") == {0: [0], 1: [1]}
    assert exec_create_int_index(TestFile4, b"000\n001\n000") == {0: [0, 2], 1: [1]}


class TestFile6:

    FIELDSPECS = [
        {"name": "ID", "len": 2},
        {"name": "sep1", "len": 1},
        {"name": "ORDER_DATE", "len": 8},   # yyyymmdd
        {"name": "sep2", "len": 1},
        {"name": "MODIFIED", "len": 14},    # yyyymmddHHMMSS
    ]

def test_fwf_cython_filter_2():
    data = b"""# Comment
01 20170101 20170102172300
02 20171231 20171231235959
03 20170108 20170108101112
04 20170128 20170128111213
05 20180101 20180101000000
"""
    assert exec_line_number(TestFile6, data) == [0, 1, 2, 3, 4]
    assert exec_line_number(TestFile6, data, [["ORDER_DATE", b"20170101", b"20180101"]]) == [0, 1, 2, 3]
    assert exec_line_number(TestFile6, data, [["ORDER_DATE", b"20170201", b"20180101"]]) == [1]
    assert exec_line_number(TestFile6, data, [["ORDER_DATE", b"20170201"]]) == [1, 4]

    # These one work by intention as well. Comparison is based on the length of the provided
    # values, rather then field length as defined in the file spec.
    assert exec_line_number(TestFile6, data, [["ORDER_DATE", None, b"2018"]]) == [0, 1, 2, 3]
    assert exec_line_number(TestFile6, data, [["ORDER_DATE", b"201701", b"201702"]]) == [0, 2, 3]
    assert exec_line_number(TestFile6, data, [["ORDER_DATE", b"2017", b"201702"]]) == [0, 2, 3]

    assert exec_line_number(TestFile6, data, [["ORDER_DATE", b"20170101", b"20180101"], ["MODIFIED", b"2017", b"201702"]]) == [0, 2, 3]


class TestFile7:

    FIELDSPECS = [
        {"name": "ID", "len": 2},
        {"name": "sep1", "len": 1},
        {"name": "VALID_FROM", "len": 8},   # yyyymmdd
        {"name": "sep2", "len": 1},
        {"name": "VALID_UNTIL", "len": 8},  # yyyymmdd
    ]

def test_fwf_cython_filter_3():
    data = b"""# Comment
01 20170101 20170131
02 20170101 20170331
03 20170201 20170227
04 20170315 20170317
05 20170410 20170510
06 20170505 20170505
"""
    assert exec_line_number(TestFile7, data) == [0, 1, 2, 3, 4, 5]
    assert exec_line_number(TestFile7, data, [["VALID_FROM", b"20170201"]]) == [2, 3, 4, 5]
    assert exec_line_number(TestFile7, data, [["VALID_FROM", b"20170201"], ["VALID_UNTIL", None, b"20170505"]]) == [2, 3]
    assert exec_line_number(TestFile7, data, [["VALID_FROM", b"20170201"], ["VALID_UNTIL", None, b"20170506"]]) == [2, 3, 5]
