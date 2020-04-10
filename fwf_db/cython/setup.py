
#
# Build module with: 
#   python setup.py build_ext --inplace
#

from setuptools import setup
from distutils.extension import Extension
from Cython.Build import cythonize
import numpy

ext_modules = [
    Extension("fwf_db_ext", sources=["fwf_db_ext.pyx"], include_dirs=[numpy.get_include()]),
]

setup(
    name='fwf_db performance extensions',
    ext_modules=cythonize(ext_modules, language_level = "3", annotate=True),
    zip_safe=False,
)
