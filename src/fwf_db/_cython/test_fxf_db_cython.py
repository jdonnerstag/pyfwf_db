#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=missing-class-docstring, missing-function-docstring

import fwf_db
import fwf_db.fwf_cython
from . import fwf_db_cython


def test_say_hello():
    assert fwf_db_cython.say_hello_to("me") == "Hello me!"


class HumanFile:

    FIELDSPECS = [
        {"name": "any", "len": 999},
    ]


def exec_fwf_cython_empty(data):

    fwf = fwf_db.FWFFile(HumanFile)
    with fwf.open(data) as fd:
        assert len(fd) == 0

        rtn = fwf_db_cython.fwf_cython(fwf,
            -1, -1, -1, -1,
            None, None, None, None,
            index=None,
            unique_index=False,
            integer_index=False
        )
        assert len(fd) == len(rtn)
        print(len(rtn), rtn)


def test_fwf_cython_empty():

    exec_fwf_cython_empty(b"")
    exec_fwf_cython_empty(b"#")
    exec_fwf_cython_empty(b"# empty")
    exec_fwf_cython_empty(b"# empty \n")
