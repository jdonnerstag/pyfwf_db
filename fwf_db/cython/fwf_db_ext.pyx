"""fwf_db is about treating large fixed width files almost like a database.

Large files may have several hundred million records. They are too large
to be loaded into memory, hence we map the file content into memory.

This Cython module is not a complete module. It just contains few extension
methods for fwf_db.

fwf_db is not a replacement for an RDBMS or analytics engine, but must
be able to handled millions of millions of lookups. To achieve reasonable
performance an (in-memory) index is needed. Creating the index requires
processing millions of records, and as validated by test cases, Cython 
can be useful in creating that index. 

Similarly we had the requirement to filter certain events, e.g. records
which were provided / updated after a certain (effective) date. Or records
which are valid during a specific period determined by table fields such as 
VALID_FROM and VALID_UNTIL. Again a tight loop executed millions of times.
"""

import collections
import ctypes
import array

import numpy
cimport numpy

from libc.string cimport strncmp, strncpy
from cpython cimport array
from libc.stdlib cimport atoi

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def say_hello_to(name):
    print(f"Hello {name}!")

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def str_to_bytes(obj):
    if isinstance(obj, str):
        obj = bytes(obj, "utf-8")

    return obj

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def iter_and_filter(fwf,
    int field1_startpos, bytes field1_start_value, int field1_endpos, bytes field1_end_value,
    int field2_startpos, bytes field2_start_value, int field2_endpos, bytes field2_end_value):
    """This is an optimized effective date and period filter, that shows 10-15x 
    performance improvements. Doesn't sound a lot but 2-3 secs vs 20-30 secs makes a 
    big difference when developing software and you need to wait for it.

    The method has certain constraints:
    - Since it is working on the raw data, the values must be bytes or strings.
    - If startpos respectively endpos == -1 it'll be ignored
    - startpos and endpos are relativ to line start
    - the field length is determined by the length of the value (bytes)
    - The comparison is pre-configured: start_value <= value <= end_value
    - Empty values in the line have predetermined meaning: beginning and end of time
    """

    field1_start_value = str_to_bytes(field1_start_value)
    field1_end_value = str_to_bytes(field1_end_value)
    field2_start_value = str_to_bytes(field2_start_value)
    field2_end_value = str_to_bytes(field2_end_value)

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

    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth

    cdef const unsigned char[:] mm_view = fwf.mm
    cdef const char* mm = <const char*>&mm_view[0]

    cdef array.array result = array.array('i', [])
    array.resize(result, fwf.reclen + 1)
    cdef int* result_ptr = result.data.as_ints

    cdef int count = 0
    cdef int irow = 0
    cdef const char* line 

    while start_pos < fsize:
        line = mm + start_pos

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
            result_ptr[count] = irow
            count += 1

        start_pos += fwidth
        irow += 1

    array.resize(result, count)    
    return result

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def get_field_data(fwf, field_name):
    """Return a numpy array with all values in the sequence read from the file"""
    
    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth
    cdef int reclen = fwf.reclen

    cdef const unsigned char[:] mm_view = fwf.mm
    cdef const char* mm = <const char*>&mm_view[0]

    # Allocate memory for index data
    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = field_slice.stop - field_slice.start
    cdef numpy.ndarray values = numpy.empty(reclen, dtype=f"S{field_size}")

    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while start_pos < fsize:

        values[irow] = ptr[0 : field_size]

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def create_index(fwf, field_name):
    """Leverage the speed of Cython (and C) which saturates the SDD when reading
    the data and thus allows to do some minimal processing in between, such as 
    updating the index. It's not perfect, it adds some delay (e.g. 6 sec), which 
    is still much better then creating the index afterwards (14 secs).
    """

    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth
    cdef int reclen = fwf.reclen

    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = field_slice.stop - field_slice.start

    cdef const unsigned char[:] mm_view = fwf.mm
    cdef const char* mm = <const char*>&mm_view[0]

    cdef values = collections.defaultdict(list)

    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while start_pos < fsize:

        values[ptr[0 : field_size]].append(irow)

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def create_unique_index(fwf, field_name):
    """For every PK find the last (sequence) entry.
    
    Leverage the speed of Cython (and C) which saturates the SDD when reading
    the data and thus allows to do some minimal processing in between, such as 
    updating the index. It's not perfect, it adds some delay (e.g. 6 sec), which 
    is still much better then creating the index afterwards (14 secs).
    """

    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth
    cdef int reclen = fwf.reclen

    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = field_slice.stop - field_slice.start

    cdef const unsigned char[:] mm_view = fwf.mm
    cdef const char* mm = <const char*>&mm_view[0]

    cdef dict values = dict()

    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while start_pos < fsize:

        # This is Python and thus rather slow. But I also don't know how
        # to optimize.
        values[ptr[0 : field_size]] = irow

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    return values

    # TODO Now we have 3 methods which only differ in a single function (more or less)

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def get_int_field_data(fwf, field_name):
    """Return a numpy array with all values in the sequence read from the file"""
    
    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth
    cdef int reclen = fwf.reclen

    cdef const unsigned char[:] mm_view = fwf.mm
    cdef const char* mm = <const char*>&mm_view[0]

    # Allocate memory for index data
    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = field_slice.stop - field_slice.start
    cdef numpy.ndarray[numpy.int64_t, ndim=1] values = numpy.empty(reclen, dtype=numpy.int64)

    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while start_pos < fsize:

        values[irow] = atoi(ptr[0 : field_size])

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    return values

    
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def create_int_index(fwf, field_name):
    """Leverage the speed of Cython (and C) which saturates the SDD when reading
    the data and thus allows to do some minimal processing in between, such as 
    updating the index. It's not perfect, it adds some delay (e.g. 6 sec), which 
    is still much better then creating the index afterwards (14 secs).
    """

    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth
    cdef int reclen = fwf.reclen

    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = field_slice.stop - field_slice.start

    cdef const unsigned char[:] mm_view = fwf.mm
    cdef const char* mm = <const char*>&mm_view[0]

    cdef values = collections.defaultdict(list)
    cdef int v 
    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while start_pos < fsize:

        v = atoi(ptr[0 : field_size])
        values[v].append(irow)

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def fwf_cython(fwf, 
    int field1_startpos, bytes field1_start_value, int field1_endpos, bytes field1_end_value,
    int field2_startpos, bytes field2_start_value, int field2_endpos, bytes field2_end_value,
    index=None, unique_index=False, integer_index=False):

    cdef int create_index = index is not None
    cdef int create_unique_index = unique_index is True
    cdef int create_integer_index = integer_index is True

    cdef index_slice
    cdef int index_start
    cdef int index_stop
    if index is not None:
        index_slice = fwf.fields[index]
        index_start = index_slice.start
        index_stop = index_slice.stop

    field1_start_value = str_to_bytes(field1_start_value)
    field1_end_value = str_to_bytes(field1_end_value)
    field2_start_value = str_to_bytes(field2_start_value)
    field2_end_value = str_to_bytes(field2_end_value)

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

    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth

    cdef const unsigned char[:] mm_view = fwf.mm
    cdef const char* mm = <const char*>&mm_view[0]

    cdef array.array result
    cdef int* result_ptr

    if not create_index:
        result = array.array('i', [])
        array.resize(result, fwf.reclen + 1)
        result_ptr = result.data.as_ints

    cdef values
    cdef bytes v_bytes
    cdef int v_int

    if index is not None:
        if create_unique_index:
            values = dict()
        else:
            values = collections.defaultdict(list)

    cdef int count = 0
    cdef int irow = 0
    cdef const char* line 

    while start_pos < fsize:
        line = mm + start_pos

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
            if not create_index:
                result_ptr[count] = irow
                count += 1
            else:
                v_bytes = line[index_start : index_stop]
                if create_int_index:
                    v_int = atoi(v_bytes)
                    if create_unique_index:
                        values[v_int] = irow
                    else:
                        values[v_int].append(irow)
                else:
                    if create_unique_index:
                        values[v_bytes] = irow
                    else:
                        values[v_bytes].append(irow)

        start_pos += fwidth
        irow += 1

    if not create_index:
        array.resize(result, count)    
        return result

    return values
