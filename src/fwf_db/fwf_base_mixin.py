#!/usr/bin/env python
# encoding: utf-8

# pylint: disable=too-few-public-methods

from typing import Tuple, Iterator

class FWFBaseMixin:
    """A mixin that implements re-usable methods required in views, indexes,
    uniques, to_pandas, ...
    """

    # pylint: disable=no-self-use
    def _index1(self, parent, field, func=None) -> Iterator[Tuple[int, bytes]]:

        field = parent.field_from_index(field)

        # If the parent view has an optimized iterator ..
        if hasattr(parent, "iter_lines_with_field"):
            gen = parent.iter_lines_with_field(field)
        else:
            sslice = parent.fields[field]
            gen = ((i, line[sslice]) for i, line in parent.iter_lines())

        # Do we need to apply any transformations...
        if func:
            gen = ((i, func(v)) for i, v in gen)

        return gen
