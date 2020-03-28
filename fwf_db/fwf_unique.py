#!/usr/bin/env python
# encoding: utf-8


class FWFUnique(object):
    """Create a list of unique (distinct) value of a field, with pure
    python means
    """
    
    def __init__(self, fwffile):
        self.fwffile = fwffile


    def unique(self, field, func=None):
        """Create a list of unique values found in 'field'.

        Use 'func' to change the value before adding it to the index, e.g. 
        str, lower, upper, int, ...
        """

        field = self.fwffile.field_from_index(field)

        # If the parent view has an optimized iterator ..
        if hasattr(self.fwffile, "iter_lines_with_field"):
            gen = self.fwffile.iter_lines_with_field(field)
        else:
            sslice = self.fwffile.fields[field]
            gen = ((i, line[sslice]) for i, line in self.fwffile.iter_lines())

        # Do we need to apply any transformations...
        if func:
            gen = ((i, func(v)) for i, v in gen)

        # Add the value if not yet present
        values = {value for _, value in gen}
        return values
