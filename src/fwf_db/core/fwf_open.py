#!/usr/bin/env python
# encoding: utf-8

from typing import Iterator, Union

from .fwf_file import FWFFile
from .fwf_multi_file import FWFMultiFile


FilesType = Union[str, bytes, list['FilesType']]

def fwf_open(filespec: type, *files: FilesType) -> FWFFile|FWFMultiFile:
    """Open a fwf file (read-only) with the file specification provided"""

    assert len(files) > 0, "You must provide at least one file name"

    if len(files) == 1 and isinstance(files[0], str|bytes):
        fwf = FWFFile(filespec)
        fwf.open(files[0])
        return fwf

    fwf = FWFMultiFile(filespec)
    for file in  _flatten(files):
        fwf.open_and_add(file)

    return fwf


def _flatten(mylist):
    for elem in mylist:
        if isinstance(elem, list):
            for subc in _flatten(elem):
                yield subc
        else:
            yield elem
