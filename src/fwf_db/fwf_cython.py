#!/usr/bin/env python
# encoding: utf-8

# A convinence wrapper around a little cython lib

from .fwf_subset import FWFSubset
from .fwf_simple_index import FWFSimpleIndex
from .fwf_simple_unique_index import FWFSimpleUniqueIndex
from ._cython.fwf_db_cython import fwf_cython

# TODO I'm wondering whether this whole file should go into the cython module?

class FWFCythonException(Exception):
    ''' FWFCythonException '''

class FWFCython:
    """Python is really nice, but for tight loops that must be executed million
    times, it's interpreter nature become apparent. This is the python frontend
    for a small Cython component that speeds up few tasks needed when processing
    fixed width files (fwf).

    - Effecient filter records on effective data and a period
    - Create an index on top of a (single) field
    - Create an unique index which contains only the last index (in sequence of
      the lines read from the the file)
    - Create an integer index where the field value has been converted into an int.
    """

    def __init__(self, fwffile):
        self.fwffile = fwffile

        if getattr(fwffile, "mm", None) is None:
            raise FWFCythonException(
                f"Parameter 'fwfile' must be of type FWFFile: {type(fwffile)}")


    def get_start_pos(self, names, idx, value):
        """ get_start_pos """

        if names is None or value is None:
            return -1
        elif isinstance(names, str):
            if names:
                return self.fwffile.fields[names].start if idx == 0 else -1
        elif isinstance(names, list) and len(names) == 2:
            names = names[idx]
            if names:
                return self.fwffile.fields[names].start
        else:
            raise FWFCythonException(f"Invalid parameter 'names: {names}")

        return -1


    def get_value(self, values, idx):
        """ get_value """

        if values is None:
            return None
        elif isinstance(values, list) and len(values) == 2:
            return values[idx]
        else:
            return values if idx == 0 else None


    def apply(self,
        field1_names=None, field1_values=None,
        field2_names=None, field2_values=None,
        index=None, unique_index=False, integer_index=False,
        index_dict=None, index_tuple=None,
        func=None):
        """ apply """

        field1_start_value = self.get_value(field1_values, 0)
        field1_stop_value = self.get_value(field1_values, 1)

        field2_start_value = self.get_value(field2_values, 0)
        field2_stop_value = self.get_value(field2_values, 1)

        field1_start_pos = self.get_start_pos(field1_names, 0, field1_start_value)
        field1_stop_pos = self.get_start_pos(field1_names, 1, field1_stop_value)

        field2_start_pos = self.get_start_pos(field2_names, 0, field2_start_value)
        field2_stop_pos = self.get_start_pos(field2_names, 1, field2_stop_value)

        if index is not None:
            index = self.fwffile.field_from_index(index)

        rtn = fwf_cython(self.fwffile,
            field1_start_pos, field1_stop_pos,
            field2_start_pos, field2_stop_pos,
            field1_start_value, field1_stop_value,
            field2_start_value, field2_stop_value,
            index=index,
            unique_index=unique_index,
            integer_index=integer_index,
            index_dict=index_dict,
            index_tuple=index_tuple
        )

        # TODO I'm wondering whether the "function" should go in the cython module?
        if (func is not None) and isinstance(rtn, dict):
            rtn = {func(k) : v for k, v in rtn.items()}

        # TODO I don't like the hard-coded Index object creation. What about methods
        # which can be subclassed?
        if index is None:
            # TODO list(rtn) is not just a wrapper. What does not require a copy?
            return FWFSubset(self.fwffile, list(rtn), self.fwffile.fields)

        if unique_index is False:
            idx = FWFSimpleIndex(self.fwffile)
            idx.field = index
            idx.data = rtn
            return idx

        idx = FWFSimpleUniqueIndex(self.fwffile)
        idx.field = index
        idx.data = rtn
        return idx
