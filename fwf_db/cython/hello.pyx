#!/usr/bin/env python
# encoding: utf-8

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

import ctypes
import array
from libc.string cimport strncmp, strncpy
from cpython cimport array
from libc.stdlib cimport malloc, free
import numpy as np

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def say_hello_to(name):
    print(f"Hello {name}!")

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def iter_and_filter(fwf,
    int field1_startpos, bytes field1_start_value, int field1_endpos, bytes field1_end_value,
    int field2_startpos, bytes field2_start_value, int field2_endpos, bytes field2_end_value):
    """This is an optimized effective date and period filter, that could also
    be implemented in C/Cython for improved performance.

    - Since it is working on the raw data, the values must be bytes.
    - If startpos respectively endpos == -1 it'll be ignored
    - startpos and endpos are relativ to line start
    - the value length is determined by the length of the value (bytes)
    - The comparison is pre-configured: start_value <= value <= end_value
    - Empty values in the line have predetermined meaning: beginning and end of time
    """

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

    cdef ptr_vdm = ctypes.c_uint.from_buffer(fwf.mm)
    cdef const char* mm = <const char*><long long>ctypes.addressof(ptr_vdm)

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
            (line[field1_end_lastpos] > 32) and
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
            (line[field2_end_lastpos] > 32) and
            (strncmp(field2_end_value, line + field2_endpos, field2_end_len) > 0)
            ):
            pass # print(f"{irow} - 4 - False")
        else:
            pass # print(f"{irow} True")
            result_ptr[count] = irow
            count += 1

        start_pos += fwidth
        irow += 1

    array.resize(result, count)    
    return result

# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def create_index(fwf, field_name):

    cdef long start_pos = fwf.start_pos
    cdef long fsize = fwf.fsize
    cdef int fwidth = fwf.fwidth

    # Get the memory map virtual address
    cdef ptr_vdm = ctypes.c_uint.from_buffer(fwf.mm)
    cdef const char* mm = <const char*><long long>ctypes.addressof(ptr_vdm)

    # Allocate memory for index data
    cdef field_slice = fwf.fields[field_name]
    cdef int field_size = field_slice.stop - field_slice.start
    cdef values = np.empty(fwf.reclen, dtype=f"S{field_size}")

    cdef int irow = 0
    cdef const char* ptr = mm + start_pos + <int>field_slice.start
    while start_pos < fsize:

        values[irow] = ptr[0 : field_size]

        start_pos += fwidth
        ptr += fwidth
        irow += 1

    return values
