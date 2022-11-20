#!/usr/bin/env python
# encoding: utf-8

from typing import Union
from pathlib import Path

from .fwf_file import FWFFile
from .fwf_multi_file import FWFMultiFile


FilesType = Union[str, bytes, Path, list['FilesType']]

def fwf_open(filespec, files: FilesType, encoding=None, newline=None, comments=None) -> FWFFile|FWFMultiFile:
    """Open a fwf file (read-only) with the file specification provided"""

    if not isinstance(files, list):
        fwf = FWFFile(filespec, encoding, newline, comments)
        fwf.open(files)
        return fwf

    fwf = FWFMultiFile(filespec, encoding, newline, comments)
    for file in  _flatten(files):
        fwf.open_and_add(file, encoding, newline, comments)

    return fwf


def _flatten(mylist):
    for elem in mylist:
        if isinstance(elem, list):
            for subc in _flatten(elem):
                yield subc
        else:
            yield elem
