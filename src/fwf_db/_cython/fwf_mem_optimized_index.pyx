#!/usr/bin/env python
# encoding: utf-8


from typing import Any, Iterator, Sequence
import collections.abc
import struct
import numpy as np


# TODO Not yet support by Cython (0.29.32)
# class BytesDictWithIntListValues(collections.abc.Mapping[Any, list[int]]):  # pylint: disable=missing-class-docstring)
class BytesDictWithIntListValues(collections.abc.Mapping):  # pylint: disable=missing-class-docstring)
    """I'm quite happy with the performance and usability of the fwf module so
    far, but the dict generated for an Index quickly grows large and consumes all
    my 24 GB memory. Just summing up the raw bytes, the ints and addresses, it
    should theoretically be possible to consume much less. I assume that Python's
    generic approach to variables is causing that. With 15 mio keys in the
    dict and multiple lineno ints, my specialised dict reduces memory consumption
    by 7 GB !!!

    This class is an attempt to overcome the challenge. Lets assume for now
    Python dicts are great and we possibly gain more by optimizing the list
    of ints associated with each index key.

    Instead of a list, the dict value is an integer pointing at a position
    within an array, e.g. dict(index, index-start-pos).

    The array is a memory efficient integer array. The array consists
    of a) the index-pos of the next elem in the list or 0 for end-of-list,.
    and b) an int for the lineno.

    This special dict is only useful for non-unique indexes. For unique
    indices a standard python dict is perfectly fine.
    """

    def __init__(self, maxsize: int):
        """Create the dict.

        A key maintains a list of integers. Maxsize does not refer to the
        number of dict keys, but to the overall number of records (lines in file).
        """

        # TODO: This class optimizes the list for memory (and performance), but
        #       the dict itself is not yet optimized. It is still consuming
        #       a lot of mem (many string or int objects)
        # What is a dict:
        #   - some logic to determine the hash code.
        #   - buckets to hold a list of entries with possibly the same hash code
        #   - equality tests to identify the real value
        # Q: how much larger needs the underlying array to be? capacity vs size
        # We have a fixed-length byte field. What's simplest approach for a proper
        # hash key? Use the first/last 64 bits (8 bytes) * 101 / 31 (or similar).
        # Most of our PKs have less or slightly larger than 8 chars.
        # Note: We don't need to copy the 'key' we only need address + length. That
        # is probably consuming all the memory.
        self.index: dict[Any, int] = {} # key -> start_pos

        maxsize += 1  # We are not using the '0' entry. 0 means end-of-list.

        # Linked list: 3 values per entry: lineno, next_pos, and end_pos
        self.data = np.zeros(maxsize, dtype=np.int32)

        self.next = np.zeros(maxsize, dtype=np.int32)

        # end_pos points at the last element in the list to perf optimize appends.
        # finish() will delete these data and free up memory no longer needed.
        self.endpos = np.zeros(maxsize, dtype=np.int32)

        # The position in the arrays where to add the next values
        self.last: int = 0

        self.finalized: bool = False


    def finish(self):
        """Once all all data have been added to the dict, it is possible to
        optimize the memory layout for faster access and reduce memory
        consumption.
        """

        if self.finalized == False:
            self.finalized = True

        self.endpos = None


    def __getitem__(self, key) -> Sequence: # list[int]:
        """Please see get() for more details. the only difference is that
        the [] selector will throw an exception if the key does not exist.
        """

        value = self.get(key)
        if value is not None:
            return value

        raise KeyError(f"Key not found: {key}")


    ## @cython.boundscheck(False)  # Deactivate bounds checking
    ## @cython.wraparound(False)   # Deactivate negative indexing.
    def __setitem__(self, key: str|int, lineno: int) -> None:
        assert self.finalized == False

        self.last += 1
        value = self.index.get(key, None)
        if value is None:
            self.index[key] = inext = self.last
            self.endpos[inext] = inext
        else:
            inext = self.endpos[value]
            self.next[inext] = inext = self.last
            self.endpos[value] = inext

        self.data[inext] = lineno


    def __iter__(self) -> Iterator:
        """Iterate over all keys in the dict."""
        return iter(self.keys())


    def __len__(self) -> int:
        """The number of keys in the dict"""
        return len(self.index)


    def __contains__(self, key) -> bool:
        return key in self.index


    def keys(self):
        return self.index.keys()


    def items(self) -> Iterator: # Iterator[tuple[Any, list[int]]]:
        for k in self.index:
            yield k, self[k]


    def values(self) -> Iterator: # Iterator[list[int]]:
        for k in self.index:
            yield self[k]


    def get(self, key, default=None) -> None | Sequence: # list[int]:
        """Get the list associated with the key. If key does not exist, then
        return 'default'. The list contains the lineno (int).

        This is a typical scenario for an index that is not unique. The key
        may refer to 1 or more records in the data set.

        For unique indices a standard python dict is all that is needed.
        """

        # Get the starting position from the dict
        inext = self.index.get(key, None)
        if inext is None:
            return default

        rtn: list[int] = []
        while inext > 0:
            lineno = int(self.data[inext])
            rtn.append(lineno)

            inext = self.next[inext]

        return rtn


    # TODO Why are these functions needed? Implementation missing?
    def __eq__(self, obj) -> bool:
        return False


    # TODO Why are these functions needed? Implementation missing?
    def __ne__(self, key) -> bool:
        return True


    def is_unique(self) -> bool:
        """Determine if all index entries refer to only 1 line. If that is
        the case, then the index is unique. Unique indices do not benefit
        from the mem optimized index: performance is worse and memory
        is wasted. For unique indices prefer a plan python dict.
        """
        return np.count_nonzero(self.data) == 0
