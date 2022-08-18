#!/usr/bin/env python
# encoding: utf-8

"""fwf_db is about (very) fast access to (very) large fixed width files
including lookups by some index-field.

Large files can have hundreds of millions of records and often are too large
to fit into memory. And even if your production system might be large enough,
the dev & test servers often are not. 'fwf_db' leverages (read-only) memory
mapping to avoid any out-of-memory issues.

fwf_db is not a replacement for an RDBMS or analytics engine, but is able
to handled efficiently millions of millions of lookups. To achieve good
performance an (in-memory) indexes is supportted. Creating these indexes
requires processing millions of records, and as validated by test cases,
compilations into native code (e.g. Cython) are useful in these tight loops
of millions of iterations.

Similarly we had the requirement to filter certain records (events), e.g.
records which were provided or updated after or before a certain date
(travel in time). Or records which are valid during a specific period
determined by record fields such as VALID_FROM and VALID_UNTIL. Again a
tight loop executing millions of times.

This Cython module is not a complete standalone module. It just contains
few extension methods which have proved worthwhile. These functions are
rather low level and not intended for end-users directly.

Focus has been on performance. E.g. we had versions that were leveraging
classes, inheritance, lambdas or generators. But the performance was always
lower. Hence we ended up with simple plain functions (and a little bit of
repetitive code).

Cython supports def, cpdef and cdef to define functions. Please see the offical
documentation for more details. With cpdef, Cython decides whether python- or
C-invocation logic gets used, which might be performance relevant. If you
want to avoid this uncertainty, prefer cdef which definitely uses C-conventions.
"""

import collections
import ctypes
from dataclasses import dataclass
import array

cimport cython

import numpy
cimport numpy

from libc.string cimport strncmp, strncpy, memcpy
from cpython cimport array
from libc.stdlib cimport atoi
from libc.stdint cimport uint32_t

from ..fwf_mem_optimized_index import BytesDictWithIntListValues

ctypedef bint bool

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def say_hello_to(name):
    """Health check"""

    print(f"Hello {name}!")

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef str_to_bytes(obj):
    """Convert string to bytes using utf-8 encoding"""

    if isinstance(obj, str):
        obj = bytes(obj, "utf-8")

    return obj

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef inline uint32_t load32(const char *p):  # char*  is allowed to alias anything
    """
    Efficiently read an uint32 from a memory location that is (potentially) misaligned.
    See https://stackoverflow.com/questions/548164/mis-aligned-pointers-on-x86
    """
    cdef uint32_t tmp
    memcpy(&tmp, p, sizeof(tmp))
    return tmp

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef const char* get_virtual_address(mm):
    """Determine the virtual memory address of a (read-only) mmap region"""

    if isinstance(mm, (str, bytes)):
        return <const char*>mm

    cdef const unsigned char[:] mm_view = mm
    return <const char*>&mm_view[0]


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cpdef int str2int(const char* line, int start, int end):
    """Efficiently convert a region of bytes into an int value

    This is a convinient function. Obviously it is possible in python as well,
    but when doing it millions times, small improvements make a difference
    as well.
    """

    if start > end:
        raise IndexError(f"'start' > 'end': {start} > {end}")

    cdef int ret = 0
    cdef bool minus = False
    cdef char ch = 0

    # Skip any leading spaces
    while start < end:
        ch = line[start]
        if ch != 0x20:
            break

        start += 1

    if start < end:
        ch = line[start]
        if ch == 45:  # '-'
            minus = True
            start += 1
        elif ch == 43:  # '+'
            start += 1

    while start < end:
        ch = line[start]
        if ch < 0x30 or ch > 0x39:
            data = line[start:end]
            raise TypeError(f"String is not an int: '{data}'")

        ret = ret * 10 + (ch - 0x30)
        start += 1

    if minus:
        ret = -ret

    return ret

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef int get_field_size(fwf, field_name):
    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = field_slice.stop - field_slice.start
    return field_size

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef struct InternalData:
    int field1_startpos
    int field1_start_len
    int field1_start_stoppos
    const char* field1_start_value

    int field1_endpos
    int field1_end_len
    int field1_end_stoppos
    int field1_end_lastpos
    const char* field1_end_value

    int field2_startpos
    int field2_start_len
    int field2_start_stoppos
    const char* field2_start_value

    int field2_endpos
    int field2_end_len
    int field2_end_stoppos
    int field2_end_lastpos
    const char* field2_end_value

    int fwidth
    int min_fwidth

    const char* mm

    int count
    int irow
    const char* line
    const char* file_end


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef InternalData init_internal_data(fwf,
    int field1_startpos = 0, bytes field1_start_value = b"", int field1_endpos = 0, bytes field1_end_value = b"",
    int field2_startpos = 0, bytes field2_start_value = b"", int field2_endpos = 0, bytes field2_end_value = b""):

    cdef InternalData params

    # Const doesn't work in Cython, but all these are essentially constants,
    # aimed to speed up execution
    params.field1_startpos = field1_startpos
    params.field1_start_len = <int>len(field1_start_value)
    params.field1_start_stoppos = field1_startpos + params.field1_start_len
    params.field1_start_value = field1_start_value

    params.field1_endpos = field1_endpos
    params.field1_end_len = <int>len(field1_end_value)
    params.field1_end_stoppos = field1_endpos + params.field1_end_len
    params.field1_end_lastpos = params.field1_end_stoppos - 1
    params.field1_end_value = field1_end_value

    params.field2_startpos = field2_startpos
    params.field2_start_len = <int>len(field2_start_value)
    params.field2_start_stoppos = field2_startpos + params.field2_start_len
    params.field2_start_value = field2_start_value

    params.field2_endpos = field2_endpos
    params.field2_end_len = <int>len(field2_end_value)
    params.field2_end_stoppos = field2_endpos + params.field2_end_len
    params.field2_end_lastpos = params.field2_end_stoppos - 1
    params.field2_end_value = field2_end_value

    # Where to start within the file, what is the file size, and line width
    params.fwidth = fwf.fwidth
    params.min_fwidth = params.fwidth - fwf.number_of_newline_bytes

    # Provide access to the (read-only) memory map
    params.mm = get_virtual_address(fwf.mm)

    params.count = 0      # Position within the target array
    params.irow = 0       # Row / line count in the file
    params.line = params.mm + <long>fwf.start_pos   # Current line
    params.file_end = params.mm + <long>fwf.fsize

    return params

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef bool cmp_start_value(const char* line, int pos, int xlen, const char* value):
    return (pos >= 0) and (strncmp(value, line + pos, xlen) < 0)

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef bool cmp_end_value(const char* line, int pos, int xlen, int lastpos, const char* value):
    return (pos >= 0) and (line[lastpos] != 32) and (strncmp(value, line + pos, xlen) > 0)

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef bool cmp_values(InternalData* params):
    if cmp_start_value(params.line, params.field1_startpos, params.field1_start_len, params.field1_start_value):
        # print(f"{irow} - 1 - False")
        return False
    elif cmp_end_value(params.line, params.field1_endpos, params.field1_end_len, params.field1_end_lastpos, params.field1_end_value):
        # print(f"{irow} - 2 - False")
        return False
    elif cmp_start_value(params.line, params.field2_startpos, params.field2_start_len, params.field2_start_value):
        # print(f"{irow} - 3 - False")
        return False
    elif cmp_end_value(params.line, params.field2_endpos, params.field2_end_len, params.field2_end_lastpos, params.field2_end_value):
        # print(f"{irow} - 4 - False")
        return False

    return True

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef next_line(InternalData* params):
    params.line += params.fwidth
    params.irow += 1

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef bool has_more_lines(InternalData* params):
    return (params.line + params.min_fwidth) < params.file_end

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def iter_and_filter(fwf,
    int field1_startpos, bytes field1_start_value, int field1_endpos, bytes field1_end_value,
    int field2_startpos, bytes field2_start_value, int field2_endpos, bytes field2_end_value):
    """This is an optimized effective date and period filter, that shows 10-15x
    performance improvements. Doesn't sound a lot but 2-3 secs vs 20-30 secs makes a
    big difference when developing software and you need to wait for it.

    The method has certain (sensible) requirements:
    - Since it is working on raw data, the values must be bytes
    - If startpos respectively endpos < 0 it'll be ignored
    - startpos and endpos are relativ to line start
    - The field length is determined by the length of the value (bytes)
    - The comparison is pre-configured: start_value <= value <= end_value
    - Empty line data (all spaces) have predetermined meaning: beginning and end of time

    Return: an array with the line indices that passed the filters
    """

    cdef InternalData params = init_internal_data(fwf,
        field1_startpos, field1_start_value, field1_endpos, field1_end_value,
        field2_startpos, field2_start_value, field2_endpos, field2_end_value)

    # The result array of indices (int). We pre-allocate the memory
    # and shrink it later (and only once). The allocated memory is a
    # sequential block of memory, and we can optimize access to the
    # respective index. The pointer gets initialize to point at the
    # first index.
    cdef array.array result = array.array('i', [])
    array.resize(result, fwf.reclen + 1)
    cdef int* result_ptr = result.data.as_ints

    while has_more_lines(&params):
        # Execute the effective data and period filters
        if cmp_values(&params):
            # This record we want to keep
            result_ptr[params.count] = params.irow
            params.count += 1

        next_line(&params)

    # Shrink the array to the actually needed size
    array.resize(result, params.count)
    return result

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def get_field_data(fwf, field_name):
    """Return a numpy array with the data from the 'field', in the
    sequence read from the file.

    array has not array of bytes or string, just numeric primitives. And I
    didn't want to build another access layer. Additionally we thought that
    Numpy and Pandas certainly have additional nice cool features that will
    help us processing the data.
    """

    cdef InternalData params = init_internal_data(fwf)

    # Allocate memory for all of the data
    # We are not converting or processing the field data in any way
    # or form => dtype for binary data types and field length
    cdef field_slice = fwf.fields[field_name]
    cdef int startpos = field_slice.start
    cdef int endpos = field_slice.end
    cdef int field_size = get_field_size(fwf, field_name)
    cdef numpy.ndarray values = numpy.empty(fwf.reclen, dtype=f"S{field_size}")

    # Loop over every line
    while has_more_lines(&params):

        # Add the field value to the numpy array
        values[params.irow] = params.line[startpos : endpos]

        next_line(&params)

    # Return the numpy array
    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def create_index(fwf, field_name):
    """Create an index (dict: values -> [indices]) for 'field'

    Leverage the speed of Cython (and C) which saturates the SDD when reading
    the data and thus allows to do some minimal processing in between, such as
    updating the index. It's not perfect, it adds some delay (e.g. 6 sec),
    however it is still much better then creating the index afterwards (14 secs).

    Return: a dict mapping values to one or more lines indices
    """

    cdef InternalData params = init_internal_data(fwf)

    # The field to create the index upon
    cdef field_slice = fwf.fields[field_name]
    cdef int startpos = field_slice.start
    cdef int endpos = field_slice.end

    # The result dict that will contain the index
    cdef values = collections.defaultdict(list)

    while has_more_lines(&params):

        # Add the value and row to the index
        values[params.line[startpos : endpos]].append(params.irow)

        next_line(&params)

    # Return the index
    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def create_unique_index(fwf, field_name):
    """Create an unique index (dict: value -> index) for 'field'.

    Note: in case the same field value is found multiple times, then the
    last will replace any previous value. This is by purpose, as in our
    use case we often need to find the last change before a certain date
    or within a specific period of time, and this is the fastest way of
    doing it.

    Return: a dict mapping the value to its last index
    """

    cdef InternalData params = init_internal_data(fwf)

    # The field to create the index from
    cdef field_slice = fwf.fields[field_name]
    cdef int startpos = field_slice.start
    cdef int endpos = field_slice.end

    # The result dict: value -> last index
    cdef dict values = dict()

    while has_more_lines(&params):

        # Update the index. This is Python and thus rather slow. But I
        # also don't know how to further optimize.
        values[params.line[startpos : endpos]] = params.irow

        next_line(&params)

    # Return the index
    return values

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def get_int_field_data(fwf, field_name):
    """Read the data for 'field', convert them into a int and store them in a
    numpy array

    Doing that millions times is a nice use case for Cython. It add's no
    measurable delay.

    Return: Numpy int64 ndarray
    """

    cdef InternalData params = init_internal_data(fwf)

    # Pre-allocate memory for the numpy array (result)
    cdef field_slice = fwf.fields[field_name]
    cdef int startpos = field_slice.start
    cdef int endpos = field_slice.end
    cdef numpy.ndarray[numpy.int64_t, ndim=1] values = numpy.empty(fwf.reclen, dtype=numpy.int64)

    while has_more_lines(&params):

        # Convert the field string data into int and add to the array
        values[params.irow] = atoi(params.line[startpos : endpos])

        next_line(&params)

    # Array of int values
    return values


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def create_int_index(fwf, field_name):
    """Like create_index() except that the 'field' is converted into a int.

    Return: dict: int(field) -> [indices]
    """

    cdef InternalData params = init_internal_data(fwf)

    # The index field
    cdef field_slice = fwf.fields[field_name]
    cdef int startpos = field_slice.start
    cdef int endpos = field_slice.end

    # The result dict: int(value) -> [indices]
    cdef values = collections.defaultdict(list)

    cdef int v
    while has_more_lines(&params):

        # Convert the field value into an int and add it to the index
        v = atoi(params.line[startpos : endpos])
        values[v].append(params.irow)

        next_line(&params)

    # The index: int(field) -> [indices]
    return values


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

@dataclass
class Parameters:
    field1_startpos: int = 0
    field1_start_value: bytes
    field1_endpos: int = 0
    field1_end_value: bytes
    field2_startpos: int = 0
    field2_start_value: bytes
    field2_endpos: int = 0
    field2_end_value: bytes

    index: str = None
    unique_index: bool = False
    integer_index: bool = False
    file_id = None

    index_dict: dict = None


## @cython.boundscheck(False)  # Deactivate bounds checking
## @cython.wraparound(False)   # Deactivate negative indexing.
def fwf_cython(fwf, args):
    int field1_startpos, bytes field1_start_value, int field1_endpos, bytes field1_end_value,
    int field2_startpos, bytes field2_start_value, int field2_endpos, bytes field2_end_value,
    index=None, unique_index=False, integer_index=False, index_dict=None, index_tuple=None):
    """Putting it all together: Filter the fwf file on an effective date and a
    period, and optionally create an index on a 'field'. The index can be optionally
    be made unique and the field value can be converted into an int.

    If index is None, the indices of the filtered lines are return in an array.
    If index is a field name and unique_index is false => dict: value -> [indices].
    If index is a field name and unique_index is true => dict: value -> last index.
    If index is a field name and integer_index is true, then convert the field value
    into an int.

    This is an optimized effective date and period filter, that shows 10-15x
    performance improvements. Doesn't sound a lot but 2-3 secs vs 20-30 secs makes a
    big difference when developing software and you need to wait for it.

    The method has certain (sensible) requirements:
    - Since it is working on the raw data, the values must be bytes or strings.
    - If startpos respectively endpos == -1 it'll be ignored
    - startpos and endpos are relativ to line start
    - the field length is determined by the length of the value (bytes)
    - The comparison is pre-configured: start_value <= value <= end_value
    - Empty values in the line have predetermined meaning: beginning and end of time

    If index is a field and 'index_dict' is provided (must be a subclass of dict), then this
    dict is updated rather then a new one generated. This is useful when creating a
    single index over multiple files. Be careful to provide the correct dict type,
    depending on whether a unique or normal index is requested.

    If index is a field and 'index_tuple' is not None (usually it is an int), then a tuple will
    be added to the dict, rather then only the index. The index will be the 2nd value
    in the tuple. This is useful when creating a single index over multiple files.
    """

    cdef InternalData params = init_internal_data(fwf,
        field1_startpos, field1_start_value, field1_endpos, field1_end_value,
        field2_startpos, field2_start_value, field2_endpos, field2_end_value)

    # Some constants which will be tested millions of times
    cdef int create_index = index is not None
    cdef int create_unique_index = unique_index is True
    cdef int create_integer_index = integer_index is True

    # If index is a field name, then determine the fields position within a line
    cdef index_slice
    cdef int index_start
    cdef int index_stop
    if index is not None:
        index_slice = fwf.fields[index]
        index_start = index_slice.start
        index_stop = index_slice.stop

    # Pre-allocate memory of the result arrary, if no indexing
    # is required
    cdef array.array result
    cdef int* result_ptr
    if not create_index:
        result = array.array('i', [])
        array.resize(result, fwf.reclen + 1)
        result_ptr = result.data.as_ints

    # If an index is requested, create an respective dict that
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

    cdef int create_tuple = index_tuple is not None

    # Enable optimizations of our memory efficient index (dict)
    cdef int is_mem_optimized_dict = isinstance(index_dict, BytesDictWithIntListValues)
    cdef int mem_dict_last
    cdef dict mem_dict_dict
    cdef int [:] mem_dict_next
    cdef int [:] mem_dict_end
    cdef signed char [:] mem_dict_file
    cdef int [:] mem_dict_lineno
    cdef int index_tuple_int
    cdef int inext
    cdef int iend

    if is_mem_optimized_dict:
        mem_dict_last = index_dict.last
        mem_dict_dict = index_dict.index
        mem_dict_next = index_dict.next
        mem_dict_end = index_dict.end
        mem_dict_file = index_dict.file
        mem_dict_lineno = index_dict.lineno
        index_tuple_int = 0 if index_tuple is None else int(index_tuple)

    while has_more_lines(&params):

        if cmp_values(&params):
            # If no index, then added the index to the result array
            if not create_index:
                result_ptr[params.count] = params.irow
                params.count += 1
            else:
                # Get the field data (as bytes)
                key = params.line[index_start : index_stop]
                if create_integer_index:
                    key = atoi(key)

                if is_mem_optimized_dict:
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
                    mem_dict_lineno[inext] = params.irow

                elif create_unique_index:
                    # Unique index: just keep the last index
                    value = (index_tuple, params.irow) if create_tuple else params.irow
                    values[key] = value
                else:
                    # Add the index to potentially already existing indices
                    value = (index_tuple, params.irow) if create_tuple else params.irow
                    values[key].append(value)

        next_line(&params)

    # If only filters are provided but no index is requested, then return
    # the array with the line indices.
    if not create_index:
        array.resize(result, params.count)
        return result

    if is_mem_optimized_dict:
        values.last = mem_dict_last

    # Else, return the index
    return values
