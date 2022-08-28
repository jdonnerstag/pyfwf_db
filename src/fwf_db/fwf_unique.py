#!/usr/bin/env python
# encoding: utf-8


import abc
from typing import Callable

from .fwf_index_like import FWFIndexLike


class FWFUniqueMixin(FWFIndexLike, abc.ABC):
    """Create a list of unique (distinct) value of a field, with pure
    python means
    """

    def unique(self, field, func: None|Callable = None):
        """Create a list of unique values found in 'field'.

        Use 'func' to change the value before adding it to the index, e.g.
        str, lower, upper, int, ...
        """

        gen = self._index1(self.fwfview, field, func)

        # Create the set() with the unique values
        return {value for _, value in gen}
