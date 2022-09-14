"""A setuptools based setup module.

See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
# pylint: disable=deprecated-module, wrong-import-order
import os
import glob
import shutil
import pathlib
from setuptools import setup, find_packages, Command
from distutils.core import Extension
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
long_description = (here / "README.rst").read_text(encoding="utf-8")

class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    CLEAN_FILES = './build ./dist ./__pycache__ **/*.c **/*.egg-info'.split(' ')

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        global here

        for path_spec in self.CLEAN_FILES:
            # Make paths absolute and relative to this path
            abs_paths = glob.glob(os.path.normpath(os.path.join(here, path_spec)), recursive=True)
            #print(path_spec, abs_paths)
            for path in [str(p) for p in abs_paths]:
                if not path.startswith(str(here)):
                    # Die if path in CLEAN_FILES is absolute + outside this directory
                    raise ValueError(f"{path} is not a path inside {str(here)}")

                print(f'removing {os.path.relpath(path)}')
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
    name="fwf_db",
    version="0.1.0",
    description="Fast fixed-width-fields File Format DB-like access",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jdonnerstag/pyfwf_db",
    author="Juergen Donnerstag",
    author_email="juergen.donnerstag@gmail.com",
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
    keywords="fixed width, file, database, read-only",
    package_dir={"": "src"},  # Why is this a dict?
    packages=find_packages(where="src"),
    python_requires=">=3.7, <4",

    # This field lists other packages that your project depends on to run.
    # Any package you put here will be installed by pip when your project is
    # installed, so they must be valid existing projects.
    install_requires=["numpy", "deprecated"],

    # List additional groups of dependencies here (e.g. development
    # dependencies). Users will be able to install these using the "extras"
    # syntax, for example:
    #
    #   $ pip install sampleproject[dev]
    extras_require={
        "dev": ["pytest", "Cython", "tox", "check-manifest", "pandas"],
    },

    # If there are data files included in your packages that need to be
    # installed, specify them here.
    package_data={
        # "sample": ["package_data.dat"],
    },

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # `pip` to create the appropriate form of executable for the target
    # platform.
    #
    # For example, the following would provide a command called `sample` which
    # executes the function `main` from this package when invoked:
    entry_points={  # Optional
        "console_scripts": [
            # "sample=sample:main",
        ],
    },
    project_urls={  # Optional
        # "Bug Reports": "https://github.com/pypa/sampleproject/issues",
        # "Funding": "https://donate.pypi.org",
        # "Say Thanks!": "http://saythanks.io/to/example",
        # "Source": "https://github.com/jdonnderstag/pyfwf_db/",
    },
    ext_modules=ext_modules,
    cmdclass={
        'clean': CleanCommand,
    },
)
