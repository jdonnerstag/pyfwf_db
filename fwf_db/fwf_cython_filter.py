#!/usr/bin/env python
# encoding: utf-8


from .fwf_subset import FWFSubset
from .cython import fwf_db_ext 


class FWFCythonFilterException(Exception):
    pass


class FWFCythonFilter(object):
    """Filter file data based on an effective date and period provided."""
    
    def __init__(self, fwffile):
        self.fwffile = fwffile

        if getattr(fwffile, "mm", None) is None:
            raise FWFCythonFilterException(
                f"Parameter 'fwfile' must be of type FWFFile: {type(fwffile)}")


    def get_start_pos(self, names, idx, value):
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
            raise FWFCythonFilterException(f"Invalid parameter 'names: {names}")

        return -1


    def get_value(self, values, idx):
        if values is None:
            return None
        elif isinstance(values, list) and len(values) == 2:
            return values[idx]
        else:
            return values if idx == 0 else None


    def filter(self, field1_names, field1_values, field2_names, field2_values):

        field1_start_value = self.get_value(field1_values, 0)
        field1_stop_value = self.get_value(field1_values, 1)

        field2_start_value = self.get_value(field2_values, 0)
        field2_stop_value = self.get_value(field2_values, 1)

        field1_start_pos = self.get_start_pos(field1_names, 0, field1_start_value)
        field1_stop_pos = self.get_start_pos(field1_names, 1, field1_stop_value)

        field2_start_pos = self.get_start_pos(field2_names, 0, field2_start_value)
        field2_stop_pos = self.get_start_pos(field2_names, 1, field2_stop_value)

        rtn = fwf_db_ext.iter_and_filter(self.fwffile,
            field1_start_pos, field1_start_value,
            field1_stop_pos, field1_stop_value,
            field2_start_pos, field2_start_value,
            field2_stop_pos, field2_stop_value,
        )

        return FWFSubset(self.fwffile, list(rtn), self.fwffile.fields)
