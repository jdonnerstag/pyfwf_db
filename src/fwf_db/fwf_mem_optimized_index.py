#!/usr/bin/env python
# encoding: utf-8

"""I'm quite happy with the performance and usability of the fwf module so
far, but the dict generated for an Index quickly grows large and consumes all
my 24 GB memory. Just summing up the raw bytes, the ints and addresses, it
should theoretically be possible to consume much less. I assume that Python's
generic approach to variables is causing that. Just consider 15 mio keys in the
dict and multiple lineno ints. This specialised dict reduced memory consumption
by 7 GB !!!

This class is an attempt to overcome the challenge. Lets assume for now
Python dicts are great and we possibly gain more by optimizing the list
of ints associated with each index key.

Instead of a list, the dict value is an integer pointing at a position
within an array, e.g. dict(index, index-start-pos).

The array is a memory efficient numpy integer array. The array consists
of a) the index-pos of the next elem in the list or 0 for end-of-list,.
and b) an int for the lineno.

This special dict is only useful for non-unique indicies. For unique
indices a standard python dict is perfectly fine.
"""

from typing import Any, Iterator
import collections.abc
import struct
import numpy as np


class BytesDictWithIntListValues(collections.abc.Mapping[Any, list[int]]):  # pylint: disable=missing-class-docstring)
    __doc__ = globals()["__doc__"]      # Apply the module doc to the class as well

    def __init__(self, maxsize: int):
        """Create the dict.

        A key maintains a list of integers. Maxsize does not refer to the
        number of dict keys, but to the overall number of records (lines in file).

        NOTE: Please be careful with changes to this class, due to the
        dependency with our Cython module. That Cython module assumes
        that certain variables exists and have specific meanings.
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
        # Note: It would be much faster if the PK field address be 64 bit aligned. Which
        # is likely almost never the case. What is the fastest way to read not-aligned
        # 64 bits? Does it make sense to add an extra comment line to align on the PK
        # field?
        # We may use 32 bit lineno or 64 bit pointers
        # After initially creating the dict, we may perform an optimization step, to
        # a) decrease mem and b) improve list read performance.
        # Create a simple "len", ["lineno"]* integer list, which should be possible
        # WITHOUT creting a new array. It should be possible in-place.
        # TODO move to _cython and combine with special source code already in cython
        self.index: dict[Any, int] = {} # key -> start_pos

        maxsize += 1  # We are not using the '0' entry. 0 means end-of-list.

        # Linked list: position of the next node. 0 for end-of-list
        # TODO In the cython module we are using array.array instead of numpy. Also for easier resize.
        #      But in array.array the itemsize is more blurred.
        self.next = np.zeros(maxsize, dtype=np.int32)

        # The last node in the list for quick additions to the list
        # Can be released when all data are loaded.
        self.end = np.zeros(maxsize, dtype=np.int32)

        # The actual dict value => line number
        self.lineno = np.zeros(maxsize, dtype=np.int32)

        # TODO This is still an incomplete implementation
        # Index finalization changes the structure to: len, followed by an int for each entry (lineno)
        #    which requires less memory and is faster to access.
        self.data = np.zeros(0, dtype=np.int32)
        self.finalized: bool = False
        self.unique: bool = False

        # The position in the arrays where to add the next values
        self.last = 0


    def finish(self):
        """Once all all data have been added to the dict, it is possible to
        release some memory that is only needed for fast appends.
        """

        if self.lineno is not None:
            maxsize = len(self.lineno) + len(self.index)
            self.data = np.zeros(maxsize, dtype=np.int32)
            idx = 0
            for key, values in self.index:
                ilen = 1
                for value in values:
                    self.data[idx + ilen] = value
                    ilen += 1

                self.data[idx] = ilen - 1
                self.index[key] = idx
                idx += ilen

        self.finalized = True
        self.next = None
        self.end = None
        self.lineno = None


    def __getitem__(self, key) -> list[int]:
        """Please see get() for more details. the only difference is that
        the [] selector will throw an exception if the key does not exist.
        """

        value = self.get(key)
        if value is not None:
            return value

        raise KeyError(f"Key not found: {key}")


    def _last_pos(self, inext: int) -> int:
        assert self.next is not None

        last = inext
        while inext > 0:
            last = inext
            inext = self.next[inext]

        return last


    def __setitem__(self, key, lineno: int) -> None:
        assert self.next is not None
        assert self.lineno is not None

        self.last += 1
        inext = self.index.get(key, None)
        if inext is None:
            self.index[key] = inext = self.last
        else:
            inext = self._last_pos(inext)
            self.next[inext] = inext = self.last

        self.lineno[inext] = lineno


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


    def items(self) -> Iterator[tuple[Any, list[int]]]:
        for k in self.index:
            yield k, self[k]


    def values(self) -> Iterator[list[int]]:
        for k in self.index:
            yield self[k]


    def get(self, key, default=None) -> None | list[int]:
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

        if self.data.size > 0:
            start = inext + 1
            stop = start + self.data[inext]
            return list(self.data[start:stop])  # TODO I only want a wrapper. No need to copy into a list


        # If not yet finalized continue with the somewhat slower approach
        assert self.lineno is not None
        assert self.next is not None

        rtn: list[int] = []
        while inext > 0:
            lineno = int(self.lineno[inext])
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
        if self.next is not None:
            return np.count_nonzero(self.next) == 0

        return self.unique


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
