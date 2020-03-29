#!/usr/bin/env python
# encoding: utf-8


from .fwf_base_mixin import FWFBaseMixin


class FWFUnique(FWFBaseMixin):
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

        gen = self._index1(self.fwffile, field, func)

        # Create the set() with the unique values
        return {value for _, value in gen}
