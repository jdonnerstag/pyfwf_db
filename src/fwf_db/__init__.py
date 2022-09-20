#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A lib to help with fixed-width files.

key features:
    - Fast: if not explicity requested, time consuming conversions will
      not take place automatically. (unfortunately still CPU/core bound and not SDD)
    - Memory efficient: by means of mmap the data are loaded into memory and released
      if and when needed - fully managed by the OS. This allows to work with data
      files which are significantly larger then the main memory in your machine.
    - May be not a big deal, but comment lines - of arbitrary length - at the
      beginning of the file are supported
    - Newlines can be any chars, including \x00 or \x01
    - The last line can have the newline missing
    - Extensible: add you own views or indexes or ... We hope you find this little
      lib useful to build your own functionalities on top.
    - Treat fixed width data like a table or database, and as if the data were
      in memory, even though they are not. E.g. [0], [-1], [:], [0:-1], etc.. They
      are called views in the lib. Currently view implementations exist for ranges
      (start, stop) and indices ([1, 4, 5, 20, 1020])
    - Find all unique (distinct) values in a column. The lib contains already
      two different implementations. One based on standard python, the
      other leveraging Numpy and Pandas.
    - The same for indexes. Already two implementations exists. The index is always
      kept in memory and allows easy and fast access to the record(s). The index
      points at the offset within the file, where the fixed-width line starts.
    - You can store (e.g. pickle) the index to disc and avoid recreating it every
      time you open the fixed width data file. The fixed width file doesn't change,
      so why should the index. That brings up an important point: the lib is meant
      for reading and processing the fixed width data. The lib currently does not
      support updating the file.
    - Efficiently filter rows (lines) or individual lines, and exlude data from
      your view which you don't need.
    - Views, indices, filters, basically everything reads from a view and produces
      a view. The new view will always be a subset of its parent. This way e.g.
      it is possible to build an index only on specific data matching some other
      criteria.
    - Lookups (joins) from one table to another are easy to implement and fast
      with indices and filters in mind. Joins as in "create a new table/view with
      the data from two more tables" has not been a requirement so far.
    - Often the data needed are in several different files, e.g. because
      every day a new one arrives and you need to process the data from the
      last X days. The lib contains a multi-file module that supports this use
      case and makes the data look like they were in one table.
    - Currently the lib is opening the file in binary mode, assuming that the
      field-length information are in number of bytes rather then chars.
      Bear in mind that with utf-8 a single char may require more then 1 byte.
    - Decoding bytes into string with an encoding is supported and can be applied
      to fields that you want to process as string.
"""

from ._cython.fwf_db_cython import line_numbers, field_data, create_index
from ._cython.fwf_mem_optimized_index import BytesDictWithIntListValues
from .fwf_dict import FWFDict
from .fwf_fieldspecs import FWFFieldSpec, FWFFileFieldSpecs
from .fwf_line import FWFLine
from .fwf_view_like import FWFViewLike
from .fwf_subset import FWFSubset
from .fwf_region import FWFRegion
from .fwf_file import FWFFile
from .fwf_multi_file import FWFMultiFile
from .fwf_index_builder_cython import FWFCythonIndexBuilder


version = (0, 1, 0, 'rc1')
__version__ = "0.1.0"
