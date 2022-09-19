#!/usr/bin/env python
# encoding: utf-8

from typing import Iterable, Any

class FWFDict(dict[Any, list[int]]):
    """A special purpose dict, a little like defaultdict(list)

    Defaultdict automatically creates an entry, if the key is yet missing
    in the dict. But e.g. in case of defaultdict(list), it is required to call
    'dict[key].append(value)' to append a value to the list. In our use case,
    (indexes) this is unwanted behavior. We rather want a unified interface
    and hence 'dict[key] = value' should do the append (rather then replace)

    Indexes exist in two flavors: unique or not-unique. Unique indexes allow
    only one 1 value (which is default dict behavior). Not-Unique index allow
    many values (a list). Indexes are read-only, except while creating the index.
    The unified interface helps to keep the the index-creation code lean and mean.
    """

    def __setitem__(self, key, value: int) -> None:
        """Create the dict entry if its yet missing and put the 'value' into a list,
        or append the value to the list.

        To keep the implementation very simple, use update() to update multiple
        entries at ones.
        """

        data = self.get(key)
        if data is None:
            data = []
            super().__setitem__(key, data)

        data.append(value)


    def set(self, key, value: int) -> None:
        """same as dict[key] = value"""

        self[key] = value


    def update(self, values: Iterable[tuple[Any, int]]) -> None:
        """Allow to set multiple entries at ones."""
        for key, value in values:
            self[key] = value