#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=deprecated-module, wrong-import-order
from distutils.core import Extension, setup
from Cython.Build import cythonize
import numpy

ext_1 = Extension(
    name="fwf_db_cython",
    sources=["fwf_db_cython.pyx"],
    include_dirs=[numpy.get_include()]
)

ext_2 = Extension(
    name="fwf_mem_optimized_index",
    sources=["fwf_mem_optimized_index.pyx"],
    include_dirs=[numpy.get_include()]
)

setup(ext_modules=cythonize([ext_1, ext_2], language_level=3))
