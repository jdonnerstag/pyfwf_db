#!/usr/bin/env python
# encoding: utf-8

"""I'm quite happy with the performance and usability of the fwf module so
far, but the dict generated for an Index quickly grows large and consume all
my 24 GB memory. Just summing up the raw bytes, the ints and addresses, it
should theoretically be possible to consume much less. I assume that Python's
generic approach to variables is causing that. Just consider 15 mio keys in the
dict and multiple tuples of (file-ID, lineno). This specialised dict reduced
my memory consumption by 7 GB !!!

This class is an attempt to overcome the challenge. Lets assume for now
Python dicts are great and we possibly gain more by optimizing the list
of ints associated with each index key.

Instead of a list, the dict value is an integer pointing at a position
within an array like dict(index, index-start-pos).

The array is a memory efficient numpy integer array. The array consists
of a) the index-pos of the next elem in the list or 0 for end-of-list,.
and b) either 1 or 2 ints for file-ID and lineno. file-ID only in case
of multi-files.

This special dict is only useful for non-unique indicies. For unique
indices a standard python dict is perfectly fine.
"""

# TODO This is almost the same as the other cython file. Is it worth it
# to maintain a 2nd copy, with plenty redundant functions? Originally we
# split it because we searched for an approach that is easily extendable,
# assuming we'll want to test more indexes of different kind. We did not
# find a really good approach so far, I think. A *.pyx file represents a
# python module, not everything that is in a directory gets merged into
# a module.


import struct
import collections

import numpy as np
cimport numpy

from libc.stdlib cimport atoi
from libc.string cimport strncmp, strncpy, memcpy

from fwf_db_cython import str_to_bytes


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def say_hello_to(name):
    """Health check"""

    return f"Hello {name}!"

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# pylint: disable=missing-class-docstring
class BytesDictWithIntListValues(collections.abc.Mapping):
    # Apply the module doc to the class as well
    __doc__ = globals()["__doc__"]

    def __init__(self, maxsize):
        """Create the dict. Maxsize does not refer to the number of
        keys in the dict, but to the number of integers across
        all the lists associated with all the keys.

        NOTE: Please be careful with changes to this class, due to the
        dependency with our Cython module. That Cython module assumes
        that certain variables exists and have specific meanings.
        """

        # TODO: We optimized the list for memory (and performance), and
        # TODO now the dict itself is next as consuming major amount of mem.
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
        # We may use 32 bit line no or 64 bit pointers
        self.index = dict() # key -> start_pos

        maxsize += 1  # We are not using the '0' entry. 0 means end-of-list.

        # Linked list: position of the next node. 0 for end-of-list
        self.next = np.zeros(maxsize, dtype="int32")

        # The last node in the list for quick additions to the list
        # Can be released when all data are loaded.
        self.end = np.zeros(maxsize, dtype="int32")

        # Every list entry is a tuple of 2 integers: file-id and lineno
        self.file = np.zeros(maxsize, dtype="int8")
        self.lineno = np.zeros(maxsize, dtype="int32")

        # The position in the arrays where to add the next values
        self.last = 0


    def finish(self):
        """Once all all data have been added to the dict, it is possible to
        release some memory that is only needed for fast appends.
        """
        self.end = None


    def resize_array(self, data, newsize):
        newdata = np.zeros(newsize, dtype=data.dtype)
        newdata[:len(data)] = data
        return newdata


    def resize(self, grow_by):
        newlen = self.last + 1 + grow_by

        if newlen > len(self.next):
            self.next = self.resize_array(self.next, newlen)
            self.end = self.resize_array(self.end, newlen)
            self.file = self.resize_array(self.file, newlen)
            self.lineno = self.resize_array(self.lineno, newlen)

        return self


    def __getitem__(self, key):
        """Please see get() for more details. the only difference is that
        the [] selector will throw an exception if the key does not exist.
        """
        if key not in self.index:
            raise KeyError(f"Key not found: {key}")

        return self.get(key)


    def __iter__(self):
        """Iterate over all items in the dict. Internally it is using
        get() to retrieve the value per key.
        """
        return self.items()


    def __len__(self):
        """The number of keys in the dict"""
        return len(self.index)


    def __contains__(self, key):
        return key in self.index


    def keys(self):
        return self.index.keys()


    def items(self):
        for k in self.index.keys():
            yield k, self.get(k)


    def values(self):
        for k in self.index.keys():
            yield self.get(k)


    def get(self, key):
        """Get the list associated with the key. If key does not exist, then
        return None. The list contains tuples which consists of two integers.

        This is a typical scenario for an index that is not unique. The key
        may refer to 1 or more records in the data set.

        For unique indices a standard python dict is all that is needed.
        """

        # Get the starting posiiton from the dict
        inext = self.index.get(key, None)
        if inext is None:
            return

        # We found an entry. Every entry (list) has at least 1 tuple
        rtn = []
        while inext > 0:
            file = int(self.file[inext])
            lineno = int(self.lineno[inext])
            rtn.append((file, lineno))

            inext = self.next[inext]

        return rtn


    def __eq__(self, obj):
        return False


    def __ne__(self, key):
        return True


    def last_pos(self, inext):
        last = inext
        while inext > 0:
            last = inext
            inext = self.next[inext]

        return last


    def __setitem__(self, key, value):
        if isinstance(value, tuple):
            file, lineno = value
        else:
            file = 0
            lineno = value

        self.last += 1
        inext = self.index.get(key, None)
        if inext is None:
            self.index[key] = inext = self.last
        else:
            inext = self.last_pos(inext)
            self.next[inext] = inext = self.last

        self.file[inext] = file
        self.lineno[inext] = lineno


    def is_unique(self):
        """Determine if all index entries refer to only 1 line. If that is
        the case, then the index is unique. Unique indices do not benefit
        from the mem optimized index: performance is worse and memory
        is wasted. For unique indices prefer a plan python dict.
        """
        return np.count_nonzero(self.next) == 0



class MyIndexDict:
    def __init__(self, size, mm, reclen, field_pos, field_len, align="left", capacity=0):
        self._size = size
        self._mm = mm
        self._reclen = reclen
        self._field_pos = field_pos
        self._field_len = field_len
        self._align = align

        self._field_len2 = min(field_len, 8)
        if align == "left":
            self._field_pos2 = field_pos
        elif align == "right":
            self._field_pos2 = field_pos + self._field_len - self._field_len2
        else:
            raise Exception(f"Invalid value for 'align': {align}. Must be 'left' or 'right'")

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


    def hash_calc(self, data):
        return 101 * data + 31


    def hash(self, key):
        if len(key) < 8:
            if self._align == "left":
                key = key.ljust(8, b"\0")
            else:
                key = key.rjust(8, b"\0")

        assert len(key) == 8

        data = struct.unpack(">Q", key)[0]
        return self.hash_calc(data)


    def bucket(self, key):
        myhash = self.hash(key)
        return myhash % self._capacity


    def _put_into_bucket(self, bucket, line_no):
        self._last += 1
        if self._last >= len(self._lineno):
            # Note: 0 is reserved, hence you effectively have len - 1 lots available !!!
            raise Exception(f"This is a fixed size dict-like and you're trying to add too many keys")

        idx = self._start[bucket]
        if idx == 0:
            self._start[bucket] = self._last
        else:
            while self._next[idx] != 0:
                idx = self._next[idx]

            self._next[idx] = self._last

        # TODO This is the unique index use case only
        self._lineno[self._last] = line_no


    def put(self, line, line_no):
        key = line[self._field_pos2 : self._field_pos2 + self._field_len2]
        bucket = self.bucket(key)

        self._put_into_bucket(bucket, line_no)


    def _get_from_bucket(self, bucket, key, default=None):
        idx = self._start[bucket]
        while idx > 0:
            # TODO This is the unique index use case only
            lineno = self._lineno[idx]

            line_pos = lineno * self._reclen
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
        """Determine how many keys are unique and the "length" for each
        """

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

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

## @cython.boundscheck(False)  # Deactivate bounds checking
## @cython.wraparound(False)   # Deactivate negative indexing.
def fwf_cython(fwf,
    int field1_startpos, int field1_endpos, int field2_startpos, int field2_endpos,
    bytes field1_start_value, bytes field1_end_value, bytes field2_start_value, bytes field2_end_value,
    index=None, unique_index=False, integer_index=False, index_dict=None, index_tuple=None):
    """
    Please see fwf_db_cython.fwf_cython() for more details. This function is
    exactly the same, but uses the memory optimized dictionary also defined in
    this package.
    """

    # Some constants which will be tested millions of times
    cdef int create_index = index is not None
    cdef int create_unique_index = unique_index is True
    cdef int create_integer_index = integer_index is True

    if not create_index:
        raise Exception("'index' is None. Please use fwf_db_cython.fwf_cython() if you don't require an index")

    if not create_unique_index:
        raise Exception("'create_unique_index' is True. Please use fwf_db_cython.fwf_cython() for unique indices")

    # If index is a field name, then determine the fields position within a line
    cdef index_slice = fwf.fields[index]
    cdef int index_start = index_slice.start
    cdef int index_stop = index_slice.stop

    # Convert the filter values into bytes if needed
    field1_start_value = str_to_bytes(field1_start_value)
    field1_end_value = str_to_bytes(field1_end_value)
    field2_start_value = str_to_bytes(field2_start_value)
    field2_end_value = str_to_bytes(field2_end_value)

    # Const doesn't work in Cython, but all these are essentially constants,
    # aimed to speed up execution
    cdef int field1_start_len = <int>(len(field1_start_value)) if field1_start_value else 0
    cdef int field1_start_stoppos = field1_startpos + field1_start_len

    cdef int field1_end_len = <int>(len(field1_end_value)) if field1_end_value else 0
    cdef int field1_end_stoppos = field1_endpos + field1_end_len
    cdef int field1_end_lastpos = field1_end_stoppos - 1

    cdef int field2_start_len = <int>(len(field2_start_value)) if field2_start_value else 0
    cdef int field2_start_stoppos = field2_startpos + field2_start_len

    cdef int field2_end_len = <int>(len(field2_end_value)) if field2_end_value else 0
    cdef int field2_end_stoppos = field2_endpos + field2_end_len
    cdef int field2_end_lastpos = field2_end_stoppos - 1

    # Where to start within the file, what is the file size, and line width
    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth

    # Get the memory address for the (memory-mapped) file
    cdef const char* mm = get_virtual_address(fwf.mm)

    # If an index is requested, create an respective dict that
    # eventually will contain the index.
    cdef values
    cdef key
    cdef value

    if index_dict is not None:
        if not isinstance(index_dict, BytesDictWithIntListValues):
            raise Exception("Parameter 'index_dict' must either be of type BytesDictWithIntListValues")

        values = index_dict
    else:
        values = BytesDictWithIntListValues()

    cdef int create_tuple = index_tuple is not None

    # Enable optimizations of our memory efficient index (dict)
    cdef int mem_dict_last = index_dict.last
    cdef dict mem_dict_dict = index_dict.index
    cdef int [:] mem_dict_next = index_dict.next
    cdef int [:] mem_dict_end = index_dict.end
    cdef signed char [:] mem_dict_file = index_dict.file
    cdef int [:] mem_dict_lineno = index_dict.lineno
    cdef int index_tuple_int = 0 if index_tuple is None else int(index_tuple)
    cdef int inext
    cdef int iend

    # Iterate over all the lines in the file
    cdef int count = 0      # The index in array to add the next index
    cdef int irow = 0       # Current line number
    cdef const char* line   # Line data

    while (start_pos + fwidth) <= fsize:
        line = mm + start_pos

        # Execute the effective data and period filters
        if (
            (field1_startpos >= 0) and
            (strncmp(field1_start_value, line + field1_startpos, field1_start_len) < 0)
            ):
            pass # print(f"{irow} - 1 - False")
        elif (
            (field1_endpos >= 0) and
            (line[field1_end_lastpos] != 32) and
            (strncmp(field1_end_value, line + field1_endpos, field1_end_len) > 0)
            ):
            pass # print(f"{irow} - 2 - False")
        elif (
            (field2_startpos >= 0) and
            (strncmp(field2_start_value, line + field2_startpos, field2_start_len) < 0)
            ):
            pass # print(f"{irow} - 3 - False")
        elif (
            (field2_endpos >= 0) and
            (line[field2_end_lastpos] != 32) and
            (strncmp(field2_end_value, line + field2_endpos, field2_end_len) > 0)
            ):
            pass # print(f"{irow} - 4 - False")
        else:
            # The line passed all the filters

            # Get the field data (as bytes)
            key = line[index_start : index_stop]
            if create_integer_index:
                key = atoi(key)

            # Cython is all about performance and this little bit of
            # code specific to the memory optimized dict for indices,
            # allows Cython to apply it's magic.

            mem_dict_last += 1
            value = mem_dict_dict.get(key, None)
            if value is None:
                inext = mem_dict_last
                mem_dict_dict[key] = inext
                mem_dict_end[inext] = inext
            else:
                iend = value
                inext = mem_dict_end[iend]
                mem_dict_next[inext] = mem_dict_last
                mem_dict_end[iend] = mem_dict_last
                inext = mem_dict_last

            mem_dict_file[inext] = index_tuple_int
            mem_dict_lineno[inext] = irow

        start_pos += fwidth
        irow += 1

    values.last = mem_dict_last

    # Else, return the index
    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef const char* get_virtual_address(mm):
    """Determine the virtual memory address of a (read-only) mmap region"""

    cdef const unsigned char[:] mm_view = mm
    cdef const char* addr = <const char*>&mm_view[0]
    return addr
