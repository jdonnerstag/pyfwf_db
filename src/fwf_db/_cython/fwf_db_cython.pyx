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

# TODO Double-check which function is still required, vs. exists for
# historic reasons.

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

cpdef int get_field_size(fwf, field_name):
    """For field 'field_name' determine its size (number of bytes) in the fixed-width files"""
    cdef field_slice = fwf.fields[field_name]
    return field_slice.stop - field_slice.start

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cpdef get_field_data(fwf, field_name):
    """Return a numpy array of string values with the data from the 'field',
    in the sequence read from the file.
    """

    # Some constants needed further down
    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth
    cdef int reclen = fwf.reclen

    # The file has been mapped into memory. Get its address.
    cdef const char* mm = get_virtual_address(fwf.mm)

    # Allocate memory for all of the data
    # We are not converting or processing the field data in any way
    # or form => dtype for binary data types and field length
    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = get_field_size(fwf, field_name)
    cdef numpy.ndarray values = numpy.empty(reclen, dtype=f"S{field_size}")

    # Loop over every line
    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while (start_pos + fwidth) <= fsize:

        # Add the field value to the numpy array
        values[irow] = ptr[0 : field_size]

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    # Return the numpy array
    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cpdef create_index(fwf, field_name):
    """Create an index (dict: value -> [line number]) for 'field'

    Leverage the speed of Cython (and C) which saturates the SDD when reading
    the data and thus allows to do some minimal processing in between, such as
    updating the index. It's not perfect, it adds some delay (e.g. 6 sec),
    however it is still much better then creating the index afterwards (14 secs).

    Return: a dict which maps values to one or more lines (line number), representing
    lines which have the same value (index key).
    """

    # Some constants
    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth
    cdef int reclen = fwf.reclen

    # The field to create the index upon
    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = get_field_size(fwf, field_name)

    # The file has been mapped into memory. Get its address.
    cdef const char* mm = get_virtual_address(fwf.mm)

    # The result dict that will contain the index
    cdef values = collections.defaultdict(list)

    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while (start_pos + fwidth) <= fsize:

        # Add the value and row to the index
        values[ptr[0 : field_size]].append(irow)

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    # Return the index
    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cpdef create_unique_index(fwf, field_name):
    """Create an unique index (dict: value -> line number) for 'field'.

    Note: in case the same field value is found multiple times, then the
    last will replace any previous value. This is by purpose, as in our
    use case we often need to find the last change before a certain date
    or within a specific period of time, and this is the fastest way of
    doing it.

    Return: a dict mapping the value to its last index
    """

    # Some constants
    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth
    cdef int reclen = fwf.reclen

    # The field to create the index from
    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = get_field_size(fwf, field_name)

    # The file has been mapped into memory. Get its address.
    cdef const char* mm = get_virtual_address(fwf.mm)

    # The result dict: value -> last index
    cdef dict values = dict()

    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while (start_pos + fwidth) <= fsize:

        # Update the index. This is Python and thus rather slow. But I
        # also don't know how to further optimize.
        values[ptr[0 : field_size]] = irow

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    # Return the index
    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cpdef get_int_field_data(fwf, field_name):
    """Read the data for 'field', convert them into a int and store them in a
    numpy array

    Doing that millions times is a nice use case for Cython. It add's no
    measurable delay.

    Return: Numpy int64 ndarray
    """

    # Some constants
    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth
    cdef int reclen = fwf.reclen

    # The file has been mapped into memory. Get its address.
    cdef const char* mm = get_virtual_address(fwf.mm)

    # Pre-allocate memory for the numpy array (result)
    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = field_slice.stop - field_slice.start
    cdef numpy.ndarray[numpy.int64_t, ndim=1] values = numpy.empty(reclen, dtype=numpy.int64)

    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while (start_pos + fwidth) <= fsize:

        # Convert the field string data into int and add to the array
        values[irow] = atoi(ptr[0 : field_size])

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    # Array of int values
    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cpdef create_int_index(fwf, field_name):
    """Like create_index() except that the 'field' is converted into a int.

    Return: dict: int(field) -> [indices]
    """

    # Some constants
    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth
    cdef int reclen = fwf.reclen

    # The index field
    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = get_field_size(fwf, field_name)

    # The file has been mapped into memory. Get its address.
    cdef const char* mm = get_virtual_address(fwf.mm)

    # The result dict: int(value) -> [indices]
    cdef values = collections.defaultdict(list)

    cdef int v
    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while (start_pos + fwidth) <= fsize:

        # Convert the field value into an int and add it to the index
        v = atoi(ptr[0 : field_size])
        values[v].append(irow)

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    # The index: int(field) -> [indices]
    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

## @cython.boundscheck(False)  # Deactivate bounds checking
## @cython.wraparound(False)   # Deactivate negative indexing.
cdef inline int last_pos(int [:] next_ar, int inext):
    cdef int last = inext
    while inext > 0:
        last = inext
        inext = next_ar[inext]

    return last

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

#-- # @cython.boundscheck(False)  # Deactivate bounds checking
#-- # @cython.wraparound(False)   # Deactivate negative indexing.
cpdef fwf_cython(fwf,
    int field1_startpos, int field1_endpos, int field2_startpos, int field2_endpos,
    bytes field1_start_value, bytes field1_end_value, bytes field2_start_value, bytes field2_end_value,
    index=None, unique_index=False, integer_index=False, index_dict=None, index_tuple=None):
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
        index_slice = fwf.fields[index]
        index_start = index_slice.start
        index_stop = index_slice.stop

    # Const doesn't work in Cython, but all these are essentially constants,
    # aimed to speed up execution
    cdef int field1_start_len = <int>(len(field1_start_value)) if field1_start_value else 0
    cdef int field1_start_stoppos = field1_startpos + field1_start_len
    cdef int field1_start_lastpos = field1_start_stoppos - 1

    cdef int field1_end_len = <int>(len(field1_end_value)) if field1_end_value else 0
    cdef int field1_end_stoppos = field1_endpos + field1_end_len
    cdef int field1_end_lastpos = field1_end_stoppos - 1

    cdef int field2_start_len = <int>(len(field2_start_value)) if field2_start_value else 0
    cdef int field2_start_stoppos = field2_startpos + field2_start_len
    cdef int field2_start_lastpos = field2_start_stoppos - 1

    cdef int field2_end_len = <int>(len(field2_end_value)) if field2_end_value else 0
    cdef int field2_end_stoppos = field2_endpos + field2_end_len
    cdef int field2_end_lastpos = field2_end_stoppos - 1

    # Where to start within the file, what is the file size, and line width
    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth

    # Get the memory address for the (memory-mapped) file
    cdef const char* mm = get_virtual_address(fwf.mm)

    # Pre-allocate memory of the result arrary, if no indexing
    # is required
    cdef array.array result
    cdef int* result_ptr
    if not create_index:
        result = array.array('i', [])
        array.resize(result, fwf.reclen + 1)
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

    while (start_pos + fwidth) <= fsize:
        line = mm + start_pos

        # Execute the effective data and period filters
        #print(f"irow={irow}, line={line}")
        #print(f" field1_startpos={field1_startpos}, field1_start_len={field1_start_len}, field1_start_value={field1_start_value}, field1_start_lastpos={field1_start_lastpos}")
        #print(f" field1_endpos={field1_endpos}, field1_end_len={field1_end_len}, field1_end_value={field1_end_value}, field1_end_lastpos={field1_end_lastpos}")
        #print(f" field2_startpos={field2_startpos}, field2_start_len={field2_start_len}, field2_start_value={field2_start_value}, field2_start_lastpos={field2_start_lastpos}")
        #print(f" field2_endpos={field2_endpos}, field2_end_len={field2_end_len}, field2_end_value={field2_end_value}, field2_end_lastpos={field2_end_lastpos}")
        if (
            (field1_startpos >= 0) and
            (line[field1_start_lastpos] != 32) and
            (strncmp(field1_start_value, line + field1_startpos, field1_start_len) > 0)
            ):
            pass # print(f"{irow} - 1 - False")
        elif (
            (field1_endpos >= 0) and
            (line[field1_end_lastpos] != 32) and
            (strncmp(field1_end_value, line + field1_endpos, field1_end_len) <= 0)
            ):
            pass # print(f"{irow} - 2 - False")
        elif (
            (field2_startpos >= 0) and
            (line[field2_start_lastpos] != 32) and
            (strncmp(field2_start_value, line + field2_startpos, field2_start_len) > 0)
            ):
            pass # print(f"{irow} - 3 - False")
        elif (
            (field2_endpos >= 0) and
            (line[field2_end_lastpos] != 32) and
            (strncmp(field2_end_value, line + field2_endpos, field2_end_len) <= 0)
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

        start_pos += fwidth
        irow += 1

    # If only filters are provided but no index is requested, then return
    # the array with the line indices.
    if not create_index:
        array.resize(result, count)
        return result

    # Else, return the index
    return values
