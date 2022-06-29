"""fwf_db is about accessing large fixed width files almost like a
(read-only) database.

Large files may have hundred millions of records and might be too large
to load into memory. Hence fwf_db maps them into memory.

This Cython module is not a complete standalone module. It just contains
few extension methods for fwf_db which have proved worthwhile. These
functions are rather low level and not intended for end-users directly.

fwf_db is not a replacement for an RDBMS or analytics engine, but must
be able to handled efficiently millions of millions of lookups. To achieve
reasonable performance an (in-memory) index is needed. Creating the index
requires processing millions of records, and as validated by test cases,
Cython is useful in these tight loops of millions of iterations.

Similarly we had the requirement to filter certain events, e.g. records
which were provided / updated after a certain (effective) date. Or records
which are valid during a specific period determined by table fields such as
VALID_FROM and VALID_UNTIL. Again a tight loop executing millions of times.
"""

import collections
import ctypes
import array

cimport cython

import numpy
cimport numpy

from libc.string cimport strncmp, strncpy, memcpy
from libc.stdint cimport uint32_t
from cpython cimport array
from libc.stdlib cimport atoi

from ..fwf_subset import FWFSubset
from ..fwf_simple_index import FWFSimpleIndex
from ..fwf_simple_unique_index import FWFSimpleUniqueIndex

ctypedef bint bool
ctypedef object FWFFile

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def say_hello_to(name):
    """Health check"""

    return f"Hello {name}!"

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cpdef str_to_bytes(obj):
    """Convert string to bytes using utf-8 encoding"""

    if isinstance(obj, str):
        obj = bytes(obj, "utf-8")

    return obj

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef uint32_t load32(const char *p):  # char*  is allowed to alias anything
    """
    Read an uint32 from a memory location that is (potentially) misaligned.
    See https://stackoverflow.com/questions/548164/mis-aligned-pointers-on-x86
    """
    cdef uint32_t tmp
    memcpy(&tmp, p, sizeof(tmp))
    return tmp

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef const char* get_virtual_address(mm):
    """Determine the virtual memory address of a (read-only) mmap region"""

    cdef const char* addr

    if isinstance(mm, (str, bytes)):
        addr = mm
        return addr

    cdef const unsigned char[:] mm_view = mm
    addr = <const char*>&mm_view[0]
    return addr

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

## @cython.boundscheck(False)  # Deactivate bounds checking
## @cython.wraparound(False)   # Deactivate negative indexing.
cdef int last_pos(int [:] next_ar, int inext):
    cdef int last = inext
    while inext > 0:
        last = inext
        inext = next_ar[inext]

    return last

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cpdef int str2int(const char* line, int start, int end):
    cdef int ret = 0

    while start < end:
        ch = line[start]
        if ch < 0x30 or ch > 0x39:
            break

        ret = ret * 10 + (ch - 0x30)
        start += 1

    return ret

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

class FWFCythonException(Exception):
    ''' FWFCythonException '''

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef class FWFFilterValue:
    cdef readonly int pos
    cdef readonly bytes value
    cdef readonly int len
    cdef readonly int last
    cdef readonly bool upper

    def __init__(self, pos, value, upper):
        self.pos = pos if isinstance(pos, int) else 0
        self.value = value
        self.upper = upper
        if self.value:
            self.len = <int>len(self.value)
            self.last = self.pos + self.len - 1
        else:
            self.len = 0
            self.last = 0

    cpdef bool filter(self, const char* line):
        return ((self.len > 0) and
            (line[self.last] != 32) and
            ((strncmp(self.value, line + self.pos, self.len) > 0) != self.upper))

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef class FWFCython:
    """Python is really nice, but for tight loops that must be executed million
    times, it's interpreted nature becomes apparent. This is the python frontend
    for a small Cython component that speeds up few tasks needed when processing
    fixed width files (fwf) efficiently.

    - Effecient filter records on effective data and a period
    - Create an index on top of a (single) field
    - Create an unique index which contains only the last index (in sequence of
      the lines read from the the file)
    - Create an integer index where the field value has been converted into an int.
    """

    cdef readonly FWFFile fwf

    cdef readonly list filters

    cdef readonly long start_pos
    cdef readonly long fsize
    cdef readonly int fwidth
    cdef readonly int reclen
    cdef readonly const char* mm_addr

    cdef readonly str field_name
    cdef readonly field_slice
    cdef readonly field_size

    cdef readonly irow
    cdef readonly count

    def __init__(self, fwffile: FWFFile, field_name: str = None):
        self.fwf = fwffile
        self.start_pos = self.fwf.start_pos
        self.fsize = self.fwf.fsize
        self.fwidth = self.fwf.fwidth
        self.reclen = self.fwf.reclen
        self.mm_addr = get_virtual_address(self.fwf.mm)

        self.filters = list()

        self.field_name = field_name
        if field_name:
            self.field_slice = self.fwf.fields[field_name]
            self.field_size = self.field_slice.stop - self.field_slice.start


    def get_start_pos(self, pos, idx: int, values) -> int:
        """ internal: Determine the start position within a line

        - return -1, if 'pos' or 'names' is None. -1 equals to "no filter"
        - if 'pos' is a list, then use 'idx' to determine the entry
        - if that entry is an int, then assume it is the start pos and return it
        - if that entry is a str, then assume it is a field name, and use to determine
          find the field, and return the field's start pos.
        - Else, throw an exception
        """

        if pos is None or values is None:
            return -1

        start_pos = pos[idx] if isinstance(pos, list) else pos

        if isinstance(start_pos, int):
            return start_pos

        if isinstance(start_pos, str):
            return self.fwf.fields[start_pos].start

        raise FWFCythonException(f"Invalid parameters 'pos: {pos}, idx: {idx}, values: '{values}'")


    def get_value(self, values, idx: int) -> bytes:
        """ internal: Determine the lower or upper bound filter value """

        if values is None:
            return None
        elif isinstance(values, (str, bytes)):
            return bytes(values)
        elif isinstance(values, list):
            try:
                return bytes(values[idx])
            except:
                return None

        raise FWFCythonException(f"Invalid parameters 'values: {values}, idx: {idx}")


    def get_field_size(self, field_name: str) -> int:
        """For field 'field_name' determine its size (number of bytes) in the fixed-width files"""
        cdef field_slice = self.fwf.fields[field_name]
        return field_slice.stop - field_slice.start


    def init_1st_filter(self, pos=None, values=None):
        value = self.get_value(values, 0)
        if value:
            pos = self.get_start_pos(pos, 0, value)
            self.filters.append(FWFFilterValue(pos, value, False))

        value = self.get_value(values, 1)
        if value:
            pos = self.get_start_pos(pos, 1, value)
            self.filters.append(FWFFilterValue(pos, value, True))


    def init_2nd_filter(self, pos=None, values=None):
        value = self.get_value(values, 0)
        if value:
            pos = self.get_start_pos(pos, 0, value)
            self.filters.append(FWFFilterValue(pos, value, False))

        value = self.get_value(values, 1)
        if value:
            pos = self.get_start_pos(pos, 1, value)
            self.filters.append(FWFFilterValue(pos, value, True))


    cdef bool filter(self, const char* line):
        for f in self.filters:
            if f.filter(line):
                return False

        return True

    cpdef evaluate_line(self, int irow, const char* line):
        """Abstract base method"""


    cpdef super_analyze(self):
        """Return a numpy array of string values with the data from the 'field',
        in the sequence read from the file.
        """

        # Loop over every line
        cdef int irow = 0
        cdef long start_pos = self.start_pos
        cdef long fwidth = self.fwidth
        cdef const char* mm = self.mm_addr
        cdef long fsize = self.fsize

        self.count = 0
        cdef const char* line
        while (start_pos + fwidth) <= fsize:
            line = mm + start_pos

            if self.filter(line):
                self.evaluate_line(irow, line)
                self.count += 1

            start_pos += fwidth
            irow += 1

        self.irow = irow

    # TODO add filter
    cpdef create_index(self, field_name: str):
        """Create an index (dict: value -> [line number]) for 'field'

        Leverage the speed of Cython (and C) which saturates the SDD when reading
        the data and thus allows to do some minimal processing in between, such as
        updating the index. It's not perfect, it adds some delay (e.g. 6 sec),
        however it is still much better then creating the index afterwards (14 secs).

        Return: a dict which maps values to one or more lines (line number), representing
        lines which have the same value (index key).
        """

        # Some constants
        cdef field_slice = self.fwf.fields[field_name]
        cdef int field_size = field_slice.stop - field_slice.start

        # The result dict that will contain the index
        cdef values = collections.defaultdict(list)

        cdef int irow = 0
        cdef long start_pos = self.fwf.start_pos
        cdef const char* ptr = self.mm_addr + <int>field_slice.start
        while (start_pos + self.fwidth) <= self.fsize:

            # Add the value and row to the index
            values[ptr[0 : field_size]].append(irow)

            start_pos += self.fwidth
            ptr += self.fwidth
            irow += 1

        # Return the index
        return values


    # TODO add filter
    cpdef dict create_unique_index(self, field_name: str):
        """Create an unique index (dict: value -> line number) for 'field'.

        Note: in case the same field value is found multiple times, then the
        last will replace any previous value. This is by purpose, as in our
        use case we often need to find the last change before a certain date
        or within a specific period of time, and this is the fastest way of
        doing it.

        Return: a dict mapping the value to its last index
        """

        # Some constants
        cdef field_slice = self.fwf.fields[field_name]
        cdef int field_size = field_slice.stop - field_slice.start

        # The result dict: value -> last index
        cdef dict values = dict()

        cdef int irow = 0
        cdef long start_pos = self.fwf.start_pos
        cdef const char* ptr = self.mm_addr + <int>field_slice.start
        while (start_pos + self.fwidth) <= self.fsize:

            # Update the index. This is Python and thus rather slow. But I
            # also don't know how to further optimize.
            values[ptr[0 : field_size]] = irow

            start_pos += self.fwidth
            ptr += self.fwidth
            irow += 1

        # Return the index
        return values


    # TODO add filter
    cpdef numpy.ndarray get_int_column_data(self, field_name: str):
        """Read the data for 'field', convert them into a int and store them in a
        numpy array

        Doing that millions times is a nice use case for Cython. It add's no
        measurable delay.

        Return: Numpy int64 ndarray
        """

        # Some constants
        cdef field_slice = self.fwf.fields[field_name]
        cdef int field_size = field_slice.stop - field_slice.start

        # Pre-allocate memory for the numpy array (result)
        cdef numpy.ndarray[numpy.int64_t, ndim=1] values = numpy.empty(self.reclen, dtype=numpy.int64)

        cdef int irow = 0
        cdef long start_pos = self.fwf.start_pos
        cdef const char* ptr = self.mm_addr + <int>field_slice.start
        while (start_pos + self.fwidth) <= self.fsize:

            # Convert the field string data into int and add to the array
            values[irow] = atoi(ptr[0 : field_size])

            start_pos += self.fwidth
            ptr += self.fwidth
            irow += 1

        # Array of int values
        return values


    # TODO add filter
    cpdef create_int_index(self, field_name: str):
        """Like create_index() except that the 'field' is converted into a int.

        Return: dict: int(field) -> [indices]
        """

        # Some constants
        cdef field_slice = self.fwf.fields[field_name]
        cdef int field_size = field_slice.stop - field_slice.start

        # The result dict: int(value) -> [indices]
        cdef values = collections.defaultdict(list)

        cdef int v
        cdef int irow = 0
        cdef long start_pos = self.fwf.start_pos
        cdef const char* ptr = self.mm_addr + <int>field_slice.start
        while (start_pos + self.fwidth) <= self.fsize:

            # Convert the field value into an int and add it to the index
            v = atoi(ptr[0 : field_size])
            values[v].append(irow)

            start_pos += self.fwidth
            ptr += self.fwidth
            irow += 1

        # The index: int(field) -> [indices]
        return values


    def apply(self,
        index=None, unique_index=False, integer_index=False,
        index_dict=None, index_tuple=None,
        func=None):
        """ apply """

        if index is not None:
            index = self.fwf.field_from_index(index)

        rtn = self.fwf_cython(
            index=index,
            unique_index=unique_index,
            integer_index=integer_index,
            index_dict=index_dict,
            index_tuple=index_tuple
        )

        # TODO I'm wondering whether the "function" should go in the cython module?
        if (func is not None) and isinstance(rtn, dict):
            rtn = {func(k) : v for k, v in rtn.items()}

        # TODO I don't like the hard-coded Index object creation. What about methods
        # which can be subclassed?
        if index is None:
            # TODO list(rtn) is not just a wrapper. What does not require a copy?
            return FWFSubset(self.fwf, list(rtn), self.fwf.fields)

        if unique_index is False:
            idx = FWFSimpleIndex(self.fwf)
            idx.field = index
            idx.data = rtn
            return idx

        idx = FWFSimpleUniqueIndex(self.fwf)
        idx.field = index
        idx.data = rtn
        return idx


    #-- # @cython.boundscheck(False)  # Deactivate bounds checking
    #-- # @cython.wraparound(False)   # Deactivate negative indexing.
    cpdef fwf_cython(self, index=None, unique_index=False, integer_index=False,
        index_dict=None, index_tuple=None):
        """Putting it all together: Filter the fwf file on an effective date and a
        period, and optionally create an index on a 'field'. The index can optionally
        be made unique and the field value can be converted into an int.

        Filters: if fieldX_(start/end)pos >= 0, then a fieldX_(start/end)_value must
        be provided. Else that filter will be ignored.

        If index is None, the indices of the filtered lines are returned in an array.
        If index is a field name and unique_index is false => dict: value -> [indices].
        If index is a field name and unique_index is true => dict: value -> last index.
        If index is a field name and integer_index is true, then convert the field value
        into an int.

        This is an optimized effective date and period filter, that shows 10-15x
        performance improvements. Doesn't sound a lot but 2-3 secs vs 20-30 secs makes a
        big difference when developing software and you need to wait for the result.

        The method has certain (sensible) requirements:
        - Since it is working on raw data, the values must be bytes
        - If startpos respectively endpos == -1 it'll be ignored
        - startpos and endpos are relativ to line start
        - the field length is determined by the length of the value (bytes)
        - The comparison is pre-configured: start_value <= value < end_value
        - Empty line values are never filtered.

        If index is a field and 'index_dict' is provided (must be a subclass of dict), then this
        dict is updated rather then a new one generated. This is useful when creating a
        single index over multiple files. Be careful to provide the correct dict type,
        depending on whether a unique or normal index is requested.

        If index is a field and 'index_tuple' is not None (usually it is an int), then a tuple will
        be added to the dict, rather then only the index. The index will be the 2nd value
        in the tuple. This is useful when creating a single index over multiple files.
        """

        # Some constants which will be tested millions of times
        cdef int create_index = index is not None
        cdef int create_unique_index = unique_index is True
        cdef int create_integer_index = integer_index is True
        cdef int create_tuple = index_tuple is not None

        # If index is a field name, then determine the fields position within a line
        cdef index_slice
        cdef int index_start
        cdef int index_stop
        if index is not None:
            index_slice = self.fwf.fields[index]
            index_start = index_slice.start
            index_stop = index_slice.stop

        # Where to start within the file, what is the file size, and line width
        cdef long start_pos = self.fwf.start_pos
        cdef long fsize = self.fwf.fsize
        cdef int fwidth = self.fwf.fwidth

        # Get the memory address for the (memory-mapped) file
        cdef const char* mm = self.mm_addr

        # Pre-allocate memory of the result arrary, if no indexing
        # is required
        cdef array.array result
        cdef int* result_ptr
        if not create_index:
            result = array.array('i', [])
            array.resize(result, self.fwf.reclen + 1)
            result_ptr = result.data.as_ints

        # If an index is requested, create a respective dict that
        # eventually will contain the index.
        cdef values
        cdef key
        cdef value
        if index is not None:
            if index_dict is not None:
                values = index_dict
            else:
                if create_unique_index:
                    values = dict()
                else:
                    values = collections.defaultdict(list)

        # Iterate over all the lines in the file
        cdef int count = 0      # The index in array to add the next index
        cdef int irow = 0       # Current line number
        cdef const char* line   # Line data

        while (start_pos + self.fwidth) <= self.fsize:
            line = mm + start_pos

            # Execute the effective date and period filters
            #print(f"irow={irow}, line={line}")
            if self.filter(line) == False:
                # The line passed all the filters

                # If no index, then added the index to the result array
                if not create_index:
                    result_ptr[count] = irow
                    count += 1
                else:
                    # Get the field data (as bytes)
                    key = line[index_start : index_stop]
                    if create_integer_index:
                        key = atoi(key)

                    if create_unique_index:
                        # Unique index: just keep the last index
                        value = (index_tuple, irow) if create_tuple else irow
                        values[key] = value
                    else:
                        # Add the index to potentially already existing indices
                        value = (index_tuple, irow) if create_tuple else irow
                        values[key].append(value)

            start_pos += self.fwidth
            irow += 1

        # If only filters are provided but no index is requested, then return
        # the array with the line indices.
        if not create_index:
            array.resize(result, count)
            return result

        # Else, return the index
        return values


cdef class FWFLineNumber(FWFCython):
    """Return a numpy array of int32 with the line numbers of the optionally
    filtered lines
    """

    cdef readonly array.array result

    def __init__(self, fwffile: FWFFile):
        FWFCython.__init__(self, fwffile)

        # Allocate memory for all of the data
        # We are not converting or processing the field data in any way
        # or form => dtype for binary data types and field length
        self.result = array.array('i', [])
        array.resize(self.result, self.reclen + 1)
        #self.result_ptr = self.result.data.as_ints

    cpdef evaluate_line(self, int irow, const char* line):
        self.result[self.count] = irow

    cpdef analyze(self):
        """Return a numpy array of string values with the data from the 'field',
        in the sequence read from the file.
        """
        self.super_analyze()
        array.resize(self.result, self.count)
        return self.result


cdef class FWFColumData(FWFCython):
    """Return a numpy array of string values with the data from the 'field',
    in the sequence read from the file.
    """

    cdef readonly numpy.ndarray result

    def __init__(self, fwffile: FWFFile, field_name: str):
        FWFCython.__init__(self, fwffile, field_name)

        # Allocate memory for all of the data
        # We are not converting or processing the field data in any way
        # or form => dtype for binary data types and field length
        self.result = numpy.empty(self.reclen, dtype=self.determine_dtype())

    cpdef str determine_dtype(self):
        return f"S{self.field_size}"

    cpdef evaluate_line(self, int irow, const char* line):
        cdef bytes data = line
        self.result[self.count] = data[self.field_slice]

    cpdef analyze(self):
        """Return a numpy array of string values with the data from the 'field',
        in the sequence read from the file.
        """
        self.super_analyze()
        return self.result


cdef class FWFIntColumnData(FWFColumData):
    """Read the data for 'field', convert them into a int and store them in a
    numpy array

    Doing that millions times is a nice use case for Cython. It add's no
    measurable delay.

    Return: Numpy int ndarray
    """

    cpdef str determine_dtype(self):
        return "i4"

    cpdef evaluate_line(self, int irow, const char* line):
        self.result[self.count] = str2int(line, self.field_slice.start, self.field_slice.stop)


cdef class FWFIndex(FWFCython):
    """Create an index (dict: value -> [line number]) for 'field'

    Leverage the speed of Cython (and C) which saturates the SDD when reading
    the data and thus allows to do some minimal processing in between, such as
    updating the index. It's not perfect, it adds some delay (e.g. 6 sec),
    however it is still much better then creating the index afterwards (14 secs).

    Return: a dict which maps values to one or more lines (line number), representing
    lines which have the same value (index key).
    """

    cdef readonly dict result
    cdef readonly bool unique_index

    def __init__(self, fwffile: FWFFile, field_name: str):
        FWFCython.__init__(self, fwffile, field_name)

        # The result dict that will contain the index
        # I'm not able to cdef collections.defaultdict ?!?
        #self.result = collections.defaultdict(list)
        self.result = dict()

        self.unique_index = False

    # Because of the different return values, this function can not be a cpdef
    def determine_key(self, int irow, const char* line):
        cdef bytes data = line
        return data[self.field_slice]

    cpdef evaluate_line(self, int irow, const char* line):
        key = self.determine_key(irow, line)
        if self.unique_index:
            self.result[key] = irow
        elif key in self.result:
            self.result[key].append(irow)
        else:
            self.result[key] = [irow]

    cpdef analyze(self):
        self.super_analyze()
        return self.result


cdef class FWFUniqueIndex(FWFIndex):
    """Create an unique index (dict: value -> line number) for 'field'.

    Note: in case the same field value is found multiple times, then the
    last will replace any previous value. This is by purpose, as in our
    use case we often need to find the last change before a certain date
    or within a specific period of time, and this is the fastest way of
    doing it.

    Return: a dict mapping the value to its last index
    """

    def __init__(self, fwffile: FWFFile, field_name: str):
        FWFIndex.__init__(self, fwffile, field_name)

        self.unique_index = True


cdef class FWFIntIndex(FWFIndex):
    """Like create_index() except that the 'field' is converted into a int.

    Return: dict: int(field) -> [indices]
    """

    # Because of the different return values, this function can not be a cpdef
    # TODO Double check the performance impact
    def determine_key(self, int irow, const char* line):
        return str2int(line, self.field_slice.start, self.field_slice.stop)
