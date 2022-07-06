"""fwf_db is about accessing large fixed width files almost like a
(read-only) database.

Large files can have hundreds of millions of records and often are too large
to fit into memory. And even if your production system might be large enough,
the dev & test servers often are not. 'fwf_db' leverages memory mapping
to avoid this issue.

fwf_db is not a replacement for an RDBMS or analytics engine, but must
be able to handled efficiently millions of millions of lookups. To achieve
reasonable performance an (in-memory) index is needed. Creating these indexes
requires processing millions of records, and as validated by test cases,
Cython is useful in these tight loops of millions of iterations.

Similarly we had the requirement to filter certain events, e.g. records
which were provided / updated after or before a certain (effective) date.
Or records which are valid during a specific period determined by table
fields such as VALID_FROM and VALID_UNTIL. Again a tight loop executing
millions of times.

This Cython module is not a complete standalone module. It just contains
few extension methods for fwf_db which have proved worthwhile. These
functions are rather low level and not intended for end-users directly.

Focus has been on performance. E.g. we had versions that were leveraging
inheritance, or lambdas or generators. Because the performance was lower
they are obviously much harder for Cython to optimize. Hence we ended up
with simple plain functions (and a little bit of repetitive code).

Cython supports def, cpdef and cdef to define functions. Please see the offical
documentation for more details. With cpdef, Cython decides whether python- or
C-invocation logic gets used, which might be performance relevant. If you
want to avoid this uncertainty, prefer cdef which definitely uses C-conventions.
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

ctypedef bint bool
ctypedef object FWFFile

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def say_hello_to(name):
    """Health check"""

    return f"Hello {name}!"

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cdef str_to_bytes(obj):
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

    if isinstance(mm, (str, bytes)):
        return <const char*>mm

    cdef const unsigned char[:] mm_view = mm
    return <const char*>&mm_view[0]

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

cpdef int str2int(const char* line, int start, int end):
    """Convert bytes into an int value"""

    cdef int ret = 0
    while start < end:
        ch = line[start]
        if ch == 0x20:
            continue

        if ch < 0x30 or ch > 0x39:
            data = line[start:end]
            raise TypeError(f"String is not an int: '{data}'")

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
    """Define lower and upper bound filters

    An interesting aspect is, that these filters don't have to be aligned with the
    boundary or size of fixed-width fields. This is especially interesting for date
    fields which you can filter e.g. very efficiently by yyyy. Or where 2 fields have
    been defined for date + time, which can be treated as one for the filter.

    The number of bytes to be compared, is determined by the length (number of bytes)
    of the provided lower / upper bound values respectively.

    'lower' bounds are inclusive. 'upper' bounds are exclusive.

    Sometimes fields are empty (spaces). Empty fields (more precisely, the most
    right byte of the field is a space) are never filtered. Reason: assuming date
    and time fields are most often subject to filters, then "empty" gets interpreted as
    'lowest' or 'highest' value possible depending on whether it is a 'lower' or
    'upper' bound filter.-
    """

    cdef readonly int pos       # Position in the line
    cdef readonly bytes value   # Lower (inclusive) or upper bound (exclusive) values
    cdef readonly int len       # len(value)
    cdef readonly int last      # Position of the last char (== line[pos + len(value) - 1])
    cdef readonly bool upper    # False, if lower bound. True, if upper bound

    def __init__(self, pos, value, upper):
        self.pos = pos if isinstance(pos, int) else 0
        self.value = value
        self.upper = upper
        if self.value and pos >= 0:
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

    cdef readonly FWFFile fwf       # The fixed-width file, including the data, field definitions etc.

    cdef readonly list filters      # TODO test if a static fixed size C-style array is faster

    cdef readonly long start_pos    # Excluding leading comments, the (byte) index where the content starts
    cdef readonly long fsize        # file size (incl. comments). Never access memory beyond
    cdef readonly int fwidth        # fixed width record size, incl. newline
    cdef readonly int records       # Number of records in the file
    cdef readonly const char* mm_addr   # The memory address of the file's content (or in mem data region)

    cdef readonly str field_name    # All indexes require a key (field name)
    cdef readonly int field_start   # start positions of the field, relativ to the line
    cdef readonly int field_stop    # stop positions of the field, relativ to the line
    cdef readonly int field_size    # The length of the field

    cdef readonly int irow          # Temp: the iterators current row number
    cdef readonly int count         # Temp: the iterators current row count, excluding filtered rows.
    cdef readonly const char* line  # Temp: the iterators current line

    def __init__(self, fwffile: FWFFile, field_name: str = None):
        self.fwf = fwffile
        self.start_pos = self.fwf.start_pos
        self.fsize = self.fwf.fsize
        self.fwidth = self.fwf.fwidth
        self.records = self.fwf.reclen
        self.mm_addr = get_virtual_address(self.fwf.mm)

        self.filters = list()

        self.field_name = field_name
        if field_name:
            field = self.fwf.fields[field_name]
            self.field_start = field.start
            self.field_stop = field.stop
            self.field_size = self.field_stop - self.field_start


    def get_start_pos(self, pos, idx: int):
        """ Internal: Determine the start position within a line

        - return -1, if 'pos' is None. -1 relates to "no filter"
        - if 'pos' is a list, then use 'idx' to determine the entry
        - if that entry is an int, then assume it is the start pos and return it
        - if that entry is a str, then assume it is a field name, and use it to determine
          the field's start pos.
        - Else, throw an exception
        """

        if pos is None:
            return -1

        start_pos = pos[idx] if isinstance(pos, list) else pos

        if isinstance(start_pos, int):
            return start_pos

        field_name = start_pos
        if isinstance(field_name, bytes):
            field_name = str(field_name, "utf-8")

        if isinstance(field_name, str):
            return self.fwf.fields[field_name].start

        raise FWFCythonException(f"Invalid parameters: pos: {pos}, idx: {idx}")


    def get_value(self, values, idx: int):
        """ Internal: Determine the lower or upper bound filter value """

        if values is None:
            return None

        value = values[idx] if isinstance(values, list) else values

        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode("utf-8")

        raise FWFCythonException(f"Invalid parameters 'values: {values}, idx: {idx}")


    def add_filter(self, pos, values, upper: bool, idx: int=0):
        value = self.get_value(values, idx)
        if value:
            pos = self.get_start_pos(pos, idx)
            if pos >= 0:
                self.filters.append(FWFFilterValue(pos, value, upper))

        return True


    cdef bytes field_value(self):
        return self.line[self.field_start : self.field_stop]


    cdef bool filter(self):
        for f in self.filters:
            if f.filter(self.line):
                return False

        return True


    cdef evaluate_line(self):
        """Abstract base method"""
        raise FWFCythonException(f"This is an abstract base implementation!!")


    cdef scan_file(self):
        """Main loop: iterate over all records and filter the lines"""

        self.irow = 0
        self.count = 0
        cdef long start_pos = self.start_pos

        while (start_pos + self.fwidth) <= self.fsize:
            self.line = self.mm_addr + start_pos

            if self.filter():
                self.evaluate_line()
                self.count += 1

            start_pos += self.fwidth
            self.irow += 1


    cpdef create_index(self):
        """Create an index (dict: value -> [line number]) for 'field'

        Leverage the speed of Cython (and C) which saturates the SDD when reading
        the data and thus allows to do some minimal processing in between, such as
        updating the index. It's not perfect, it adds some delay (e.g. 6 sec),
        however it is still much better then creating the index afterwards (14 secs).

        Return: a dict which maps values to one or more lines (line number), representing
        lines which have the same value (index key).
        """

        # The result dict that will contain the index
        cdef values = collections.defaultdict(list)

        self.irow = 0
        self.count = 0
        cdef long start_pos = self.start_pos

        cdef const char* line
        while (start_pos + self.fwidth) <= self.fsize:
            line = self.mm_addr + start_pos

            if self.filter():
                # Add the value and row to the index
                values[self.field_value()].append(self.irow)

                self.count += 1

            start_pos += self.fwidth
            self.irow += 1

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
        cdef numpy.ndarray[numpy.int64_t, ndim=1] values = numpy.empty(self.records, dtype=numpy.int64)

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
            array.resize(result, self.fwf.records + 1)
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
        while (start_pos + self.fwidth) <= self.fsize:
            self.line = self.mm_addr + start_pos

            # Execute the effective date and period filters
            #print(f"irow={irow}, line={line}")
            if self.filter() == False:
                # The line passed all the filters

                # If no index, then added the index to the result array
                if not create_index:
                    result_ptr[self.count] = self.irow
                else:
                    # Get the field data (as bytes)
                    key = self.line[index_start : index_stop]
                    if create_integer_index:
                        key = atoi(key)

                    if create_unique_index:
                        # Unique index: just keep the last index
                        value = (index_tuple, self.irow) if create_tuple else self.irow
                        values[key] = value
                    else:
                        # Add the index to potentially already existing indices
                        value = (index_tuple, self.irow) if create_tuple else self.irow
                        values[key].append(value)

            start_pos += self.fwidth
            self.irow += 1

        # If only filters are provided but no index is requested, then return
        # the array with the line indices.
        if not create_index:
            array.resize(result, self.count)
            return result

        # Else, return the index
        return values


cdef class FWFLineNumber(FWFCython):
    """Return an array.array of ints with the line numbers of optionally
    filtered lines.

    This function is only of limited value. Because scanning the file content
    saturates I/O, it is benefical (performance) to index while scanning the
    file content. "indexing while scanning" is more then twice as fast as "scan,
    then index". This is also because "scan then index" needs to run through
    the file content twice.
    """

    cdef readonly array.array result

    def __init__(self, fwffile: FWFFile):
        FWFCython.__init__(self, fwffile)

        # Allocate memory for all of the data
        self.result = array.array('i', [])
        array.resize(self.result, self.records + 1)
        #self.result_ptr = self.result.data.as_ints

    cdef evaluate_line(self):
        self.result[self.count] = self.irow

    cpdef analyze(self):
        self.scan_file()
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
        self.result = numpy.empty(self.records, dtype=self.determine_dtype())

    cpdef str determine_dtype(self):
        return f"S{self.field_size}"

    cdef evaluate_line(self):
        self.result[self.count] = self.field_value()

    cpdef analyze(self):
        self.scan_file()
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

    cdef evaluate_line(self):
        self.result[self.count] = str2int(self.line, self.field_start, self.field_stop)


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

    def __init__(self, fwffile: FWFFile, field_name: str):
        FWFCython.__init__(self, fwffile, field_name)

        self.result = dict()

    cdef evaluate_line(self):
        cdef bytes key = self.field_value()
        cdef rv = self.result.get(key)
        if rv is None:
            self.result[key] = rv = []

        rv.append(self.irow)

    cpdef analyze(self):
        self.scan_file()
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

    cdef evaluate_line(self):
        self.result[self.field_value()] = self.irow


cdef class FWFIntIndex(FWFIndex):
    """Like create_index() except that the 'field' is converted into a int.

    Return: dict: int(field) -> [indices]
    """

    cdef evaluate_line(self):
        cdef int key = str2int(self.line, self.field_start, self.field_stop)
        cdef rv = self.result.get(key)
        if rv is None:
            self.result[key] = rv = []

        rv.append(self.irow)


cdef class FWFIntUniqueIndex(FWFIntIndex):

    cdef evaluate_line(self):
        cdef int key = str2int(self.line, self.field_start, self.field_stop)
        self.result[key] = self.irow
