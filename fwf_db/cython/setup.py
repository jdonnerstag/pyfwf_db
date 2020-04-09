
#
# Build module with: 
#   python setup.py build_ext --inplace
#

from setuptools import setup
from distutils.extension import Extension
from Cython.Build import cythonize

ext_modules = [
    Extension("fwf_db_ext", sources=["fwf_db_ext.pyx"]),
]

setup(
    name='fwf_db performance extensions',
    ext_modules=cythonize(ext_modules, language_level = "3", annotate=True),
    zip_safe=False,
)
