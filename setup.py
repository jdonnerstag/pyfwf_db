#!/usr/bin/env python
# encoding: utf-8

"""A setuptools based setup module."""

# pylint: disable=missing-function-docstring
import os
import glob
import shutil
import pathlib
from distutils.core import Extension        # pylint: disable=deprecated-module
from setuptools import setup, Command
from Cython.Build import cythonize
import numpy

ext_1 = Extension(
    name="fwf_db._cython.fwf_db_cython",
    sources=["src/fwf_db/_cython/fwf_db_cython.pyx"],
    include_dirs=[numpy.get_include()]
)

ext_2 = Extension(
    name="fwf_db._cython.fwf_mem_optimized_index",
    sources=["src/fwf_db/_cython/fwf_mem_optimized_index.pyx"],
    include_dirs=[numpy.get_include()]
)

ext_modules=cythonize([ext_1, ext_2], language_level=3)


here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")

class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    CLEAN_FILES = ["./build", "./_build", "./dist", "./__pycache__", "**/*.c", "**/*.egg-info"]

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        for path_spec in self.CLEAN_FILES:
            # Make paths absolute and relative to this path
            abs_paths = glob.glob(os.path.normpath(os.path.join(here, path_spec)), recursive=True)
            #print(path_spec, abs_paths)
            for path in [str(p) for p in abs_paths]:
                if not path.startswith(str(here)):
                    # Die if path in CLEAN_FILES is absolute + outside this directory
                    raise ValueError(f"{path} is not a path inside {str(here)}")

                print(f'Removing {os.path.relpath(path)}')
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

# And where it will live on PyPI: https://pypi.org/project/sampleproject/
#
# There are some restrictions on what makes a valid project name
# specification here:
# https://packaging.python.org/specifications/core-metadata/#name
setup(
    version = "0.1.0",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 4 - Beta",

        # Indicate who your project is intended for
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        'Natural Language :: English',

        "Topic :: Software Development :: Libraries",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    ext_modules=ext_modules,
    cmdclass={
        'clean': CleanCommand,
    },
)
