#!/usr/bin/env python
# encoding: utf-8

"""I'm quite happy with the performance and usability the module so far, 
but the dict generated for an Index quickly grows large and consume all 
my 24 GB memory. Just summing up the raw bytes, the ints and addresses, it 
should theoretically be possible to consume much less (factor 10 I would guess).
I assume that Python's generic approach to variables is causing that, 
assuming 15 mio keys in the dict and multiple tuples of (file-ID, lineno).

This class is an attempt to overcome the challenge. Lets assume for now 
Python dicts are great and we possibly gain more by optimizing the list 
of ints associated with each index key.

Instead of a list we the dict value is an integer pointing at a position
within an array like dict(index, index-start-pos). 

The array is a memory efficient numpy integer array. The array consists 
of a) the index-pos of the next elem in the list or 0 for end-of-list,. 
and b) either 1 or 2 ints for file-ID and lineno. file-ID only in case 
of multi-files.
"""

import collections
import numpy as np


class BytesDictWithIntListValues(collections.abc.Mapping):
    # Apply the module doc to the class as well
    __doc__ = globals()["__doc__"]

    def __init__(self, maxsize, unique=False):
        """Create the dict. Maxsize does not refer to the number of 
        keys in the dict, but to the number of integers across
        all the lists associated with all the keys.
        """
        self.index = dict() # key -> start_pos

        maxsize += 1  # We are not using the '0' entry. 0 means end-of-list.

        # Linked list: position of the next node. 0 for end-of-list
        self.next = np.zeros(maxsize, dtype="int32")

        # Every list entry is a tuple of 2 integers: file-id and lineno
        self.file = np.zeros(maxsize, dtype="int8")
        self.lineno = np.zeros(maxsize, dtype="int32")

        # In our use case "unique" means the last one found. There might 
        # be multiple in our Change-Data-Capture stream, but we are only
        # interested in the latest. With 'unique' it no longer is a list,
        # but only a single tuple. Adding a value to the list, will 
        # replace the existing one, and get() will only return a tuple, 
        # and not a list of tuples.
        self.unique = unique

        # The position in the arrays where to add the next values
        self.last = 0


    def resize_array(self, data, newsize):
        newdata = np.zeros(newsize, dtype=data.dtype)
        newdata[:len(data)] = data
        return newdata


    def resize(self, grow_by):
        newlen = self.last + 1 + grow_by

        if newlen > len(self.next):
            self.next = self.resize_array(self.next, newlen)
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
        """If unique = False, then get the list associated with the key. If key 
        does not exist, then return None. The list contains tuples which consists 
        of two integers. 

        This is a typical scenario for an index that is not unique. The key
        may refer to 1 or more records in the data set.

        If unique = True, only the tuple will be return, as the list always only
        consists of one tuple. The last tuple that was added to the dict the same
        key.
        """

        # Get the starting posiiton from the dict
        inext = self.index.get(key, None)
        if inext is None:
            return

        if self.unique:
            file = int(self.file[inext])
            lineno = int(self.lineno[inext])
            return (file, lineno)

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

        inext = self.index.get(key, None)
        if inext is None:
            self.last += 1
            self.index[key] = inext = self.last
        elif not self.unique:
            self.last += 1
            inext = self.last_pos(inext)
            self.next[inext] = inext = self.last

        if inext > len(self.file):
            raise IndexError(f"Underlying array is running out of slots => resize()")

        self.file[inext] = file
        self.lineno[inext] = lineno

