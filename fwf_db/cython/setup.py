
#
# Build module with: 
#   python setup.py build_ext --inplace
#

from setuptools import setup
from distutils.extension import Extension
from Cython.Build import cythonize

ext_modules = [
    Extension("hello", sources=["hello.pyx"]),
]

setup(
    name='Hello world app',
    ext_modules=cythonize(ext_modules, language_level = "3", annotate=True),
    zip_safe=False,
)
