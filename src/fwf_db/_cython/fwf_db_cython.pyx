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

from ..fwf_file import FWFFile
from ..fwf_subset import FWFSubset
from ..fwf_simple_index import FWFSimpleIndex
from ..fwf_simple_unique_index import FWFSimpleUniqueIndex

ctypedef bint bool

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

cdef class Function:

    cpdef evaluate(self, int irow, bytes line):
        '''  '''

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef class AdderFunction(Function):

    cdef field_slice
    cdef numpy.ndarray result

    def __init__(self, int reclen, field_slice):
        # Allocate memory for all of the data
        # We are not converting or processing the field data in any way
        # or form => dtype for binary data types and field length
        self.field_slice = field_slice
        cdef int field_size = field_slice.stop - field_slice.start
        self.result = numpy.empty(reclen, dtype=f"S{field_size}")

    cpdef evaluate(self, int irow, bytes line):
        self.result[irow] = line[self.field_slice]

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

class FWFCythonException(Exception):
    ''' FWFCythonException '''

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

    cdef readonly object fwf  # FWFFile

    cdef readonly int field1_startpos
    cdef readonly int field1_endpos
    cdef readonly int field2_startpos
    cdef readonly int field2_endpos

    cdef readonly bytes field1_start_value
    cdef readonly bytes field1_end_value
    cdef readonly bytes field2_start_value
    cdef readonly bytes field2_end_value

    cdef readonly long start_pos
    cdef readonly long fsize
    cdef readonly int fwidth
    cdef readonly int reclen
    cdef readonly const char* mm_addr


    def __init__(self, fwffile: FWFFile):
        self.fwf = fwffile
        self.start_pos = self.fwf.start_pos
        self.fsize = self.fwf.fsize
        self.fwidth = self.fwf.fwidth
        self.reclen = self.fwf.reclen
        self.mm_addr = get_virtual_address(self.fwf.mm)

        self.field1_startpos = -1
        self.field1_endpos = -1
        self.field2_startpos = -1
        self.field2_endpos = -1


    def get_start_pos(self, names, idx: int, values) -> int:
        """ internal: Determine the start position within a line

        - return -1, if 'names' or 'value' is None. A start pos of -1 will be ignored.
        - If 'names' is a string, then field name will 'names'
        - If 'names' is a list with exactly 2 entries, then apply 'idx' to determine
          the field name
        - With a valid field name determine the fields start position.
        - Else, throw an exception
        """

        name = None
        if names is None or values is None:
            return -1
        elif isinstance(names, str):
            name = names
        elif isinstance(names, list) and len(names) == 2 and 0 <= idx <= 1:
            name = names[idx]

        if name:
            return self.fwffile.fields[name].start

        raise FWFCythonException(f"Invalid parameters 'names: {names}, idx: {idx}, values: '{values}'")


    def get_value(self, values, idx: int) -> str:
        """ internal: Determine the lower or upper bound filter value """

        if values is None:
            return None
        elif isinstance(values, str):
            return values
        elif isinstance(values, list) and len(values) == 2 and 0 <= idx <= 1:
            return values[idx]

        raise FWFCythonException(f"Invalid parameters 'values: {values}, idx: {idx}")


    def get_field_size(self, field_name: str) -> int:
        """For field 'field_name' determine its size (number of bytes) in the fixed-width files"""
        cdef field_slice = self.fwf.fields[field_name]
        return field_slice.stop - field_slice.start


    def init_1st_filter(self, names=None, values=None):
        self.field1_start_value = self.get_value(values, 0)
        self.field1_stop_value = self.get_value(values, 1)

        self.field1_start_pos = self.get_start_pos(names, 0, self.field1_start_value)
        self.field1_stop_pos = self.get_start_pos(names, 1, self.field1_stop_value)


    def init_2nd_filter(self, names=None, values=None):
        self.field2_start_value = self.get_value(values, 0)
        self.field2_stop_value = self.get_value(values, 1)

        self.field2_start_pos = self.get_start_pos(names, 0, self.field2_start_value)
        self.field2_stop_pos = self.get_start_pos(names, 1, self.field2_stop_value)


    cdef bool filter(self, bytes line):
        return True


    cdef main_loop (self, adder):
        """Return a numpy array of string values with the data from the 'field',
        in the sequence read from the file.
        """

        # Loop over every line
        cdef int irow = 0
        cdef long start_pos = self.start_pos
        cdef long fwidth = self.fwidth
        cdef const char* mm = self.mm_addr
        cdef long fsize = self.fsize

        while (start_pos + fwidth) <= fsize:
            line = mm + start_pos

            if self.filter(line):
                adder(irow, line)

            start_pos += fwidth
            irow += 1


    # TODO add filter
    cpdef numpy.ndarray get_column_data(self, field_name: str):
        """Return a numpy array of string values with the data from the 'field',
        in the sequence read from the file.
        """

        cdef f = AdderFunction(self.reclen, self.fwf.fields[field_name])
        self.main_loop(f)

        # Return the numpy array
        return f.result


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
            index = self.fwffile.field_from_index(index)

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
            return FWFSubset(self.fwffile, list(rtn), self.fwffile.fields)

        if unique_index is False:
            idx = FWFSimpleIndex(self.fwffile)
            idx.field = index
            idx.data = rtn
            return idx

        idx = FWFSimpleUniqueIndex(self.fwffile)
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

        # Const doesn't work in Cython, but all these are essentially constants,
        # aimed to speed up execution
        cdef int field1_start_len = <int>(len(self.field1_start_value)) if self.field1_start_value else 0
        cdef int field1_end_len = <int>(len(self.field1_end_value)) if self.field1_end_value else 0

        cdef int field2_start_len = <int>(len(self.field2_start_value)) if self.field2_start_value else 0
        cdef int field2_end_len = <int>(len(self.field2_end_value)) if self.field2_end_value else 0

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

            # Execute the effective data and period filters
            #print(f"irow={irow}, line={line}")
            #print(f" field1_startpos={field1_startpos}, field1_start_len={field1_start_len}, field1_start_value={field1_start_value}, field1_start_lastpos={field1_start_lastpos}")
            #print(f" field1_endpos={field1_endpos}, field1_end_len={field1_end_len}, field1_end_value={field1_end_value}, field1_end_lastpos={field1_end_lastpos}")
            #print(f" field2_startpos={field2_startpos}, field2_start_len={field2_start_len}, field2_start_value={field2_start_value}, field2_start_lastpos={field2_start_lastpos}")
            #print(f" field2_endpos={field2_endpos}, field2_end_len={field2_end_len}, field2_end_value={field2_end_value}, field2_end_lastpos={field2_end_lastpos}")
            if (
                (self.field1_startpos >= 0) and
                (line[self.field1_start_lastpos] != 32) and
                (strncmp(self.field1_start_value, line + self.field1_startpos, field1_start_len) > 0)
                ):
                pass # print(f"{irow} - 1 - False")
            elif (
                (self.field1_endpos >= 0) and
                (line[self.field1_end_lastpos] != 32) and
                (strncmp(self.field1_end_value, line + self.field1_endpos, field1_end_len) <= 0)
                ):
                pass # print(f"{irow} - 2 - False")
            elif (
                (self.field2_startpos >= 0) and
                (line[self.field2_start_lastpos] != 32) and
                (strncmp(self.field2_start_value, line + self.field2_startpos, field2_start_len) > 0)
                ):
                pass # print(f"{irow} - 3 - False")
            elif (
                (self.field2_endpos >= 0) and
                (line[self.field2_end_lastpos] != 32) and
                (strncmp(self.field2_end_value, line + self.field2_endpos, field2_end_len) <= 0)
                ):
                pass # print(f"{irow} - 4 - False")
            else:
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
