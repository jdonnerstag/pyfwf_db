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
        # end_pos points at the last element in the list to perf optimize appends.
        self.data = np.zeros(maxsize * 2, dtype=np.int32)

        # end_pos points at the last element in the list to perf optimize appends.
        # finish() will delete these data and free up memory no longer needed.
        self.endpos = np.zeros(maxsize, dtype=np.int32)

        # The position in the arrays where to add the next values
        self.last = 0

        self.finalized: bool = False
        self.unique: bool = False


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

        self.last += 3
        value = self.index.get(key, None)
        if value is None:
            self.index[key] = inext = self.last
            self.endpos[inext] = inext
        else:
            inext = self.endpos[value]
            self.data[inext + 1] = inext = self.last
            self.endpos[value] = inext

        self.data[inext + 0] = lineno


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

        if self.finalized == True:
            start = inext + 1
            stop = start + self.data[inext]
            return self.data[start:stop]  # TODO I only want a wrapper. No need to copy into a list


        rtn: list[int] = []
        while inext > 0:
            lineno = int(self.data[inext + 0])
            rtn.append(lineno)

            inext = self.data[inext + 1]

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
        for i in range(3, self.last, 2):
            if self.data[i] != 0:
                return False

        return True

# ----------------------------------------------------------------------------
# ----------------------------------------------------------------------------

class MyIndexDict:

    def __init__(self, size: int, mm, line_count: int, field_pos: int, field_len: int, align: str="left", capacity: int=0):
        self._size = size
        self._mm = mm
        self._line_count = line_count
        self._field_pos = field_pos
        self._field_len = field_len
        self._align = align

        self._field_len2 = min(field_len, 8)
        if align == "left":
            self._field_pos2 = field_pos
        elif align == "right":
            self._field_pos2 = field_pos + self._field_len - self._field_len2
        else:
            raise ValueError(f"Invalid value for 'align': {align}. Must be 'left' or 'right'")

        if self._field_len <= 4:
            size = min(size, pow(2, self._field_len * 8))

        self._capacity = capacity or int(size / 0.75)
        assert self._capacity >= size

        # The hash is used to determine the index. The value is
        # the entry into the linked list of hash-equal entries.
        self._start = np.zeros(self._capacity, dtype="int32")

        # The index of the next entry (linked list). 0 if end-of-list
        self._next = np.zeros(size + 1, dtype="int32")

        # Unique index => line no
        # non-unique index => start index into linked list of equal key values
        self._lineno = np.zeros(size + 1, dtype="int32")

        # The last index where a value has been added. Incremented by 1
        # for a new entry. 0 is un-used. It means end-of-list.
        self._last = 0


    def hash_calc(self, data) -> int:
        return 101 * data + 31


    def hash(self, key) -> int:
        if len(key) < 8:
            if self._align == "left":
                key = key.ljust(8, b"\0")
            else:
                key = key.rjust(8, b"\0")

        assert len(key) == 8

        data = struct.unpack(">Q", key)[0]
        return self.hash_calc(data)


    def bucket(self, key) -> int:
        myhash = self.hash(key)
        return myhash % self._capacity


    def _put_into_bucket(self, bucket, line_no: int) -> None:
        self._last += 1
        if self._last >= len(self._lineno):
            # Note: 0 is reserved, hence you effectively have len - 1 lots available !!!
            raise Exception("This is a fixed size dict-like and you're trying to add too many keys")

        idx = self._start[bucket]
        if idx == 0:
            self._start[bucket] = self._last
        else:
            while self._next[idx] != 0:
                idx = self._next[idx]

            self._next[idx] = self._last

        # TODO This is the unique index use case only
        self._lineno[self._last] = line_no


    def put(self, line: bytes, line_no: int) -> None:
        key = line[self._field_pos2 : self._field_pos2 + self._field_len2]
        bucket = self.bucket(key)

        self._put_into_bucket(bucket, line_no)


    def _get_from_bucket(self, bucket, key, default=None):
        idx = self._start[bucket]
        while idx > 0:
            # TODO This is the unique index use case only
            lineno = self._lineno[idx]

            line_pos = lineno * self._line_count
            start = line_pos + self._field_pos
            end = start + self._field_len
            value = self._mm[start : end]

            if key == value:
                return lineno

            idx = self._next[idx]

        return default


    def get(self, key, default=None):
        if len(key) > 8:
            if self._align == "left":
                key = key[:8]
            else:
                key = key[-8:]

        bucket = self.bucket(key)

        return self._get_from_bucket(bucket, key, default)


    def analyze(self):
        """Determine how many keys are unique and the "length" for each"""

        buckets_length = dict()     # bucket-index => length

        for i in range(self._capacity):
            start = self._start[i]
            if start:
                buckets_length[i] = 1
                xnext = self._next[start]
                while xnext:
                    buckets_length[i] += 1
                    xnext = self._next[xnext]

        percentage_filled = int(len(buckets_length) * 100  / self._capacity)
        max_length = max(buckets_length.values())

        buckets_by_length = dict()
        for _, v in buckets_length.items():
            if v not in buckets_by_length:
                buckets_by_length[v] = 1
            else:
                buckets_by_length[v] += 1

        return (percentage_filled, buckets_by_length, max_length, buckets_length)
