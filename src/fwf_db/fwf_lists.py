#!/usr/bin/env python
# encoding: utf-8

from typing import Iterator, List, Iterable


class FWFList(List[bytes]):
    """Python array allows for memory optimized arrays. But unfortunately
    integer only. FWFList provides a memory efficient list for bytes of equal size.

    The list maintains a 'count' to append values. __getitem__() and __setitem__()
    provide access to any value in the array, irrespective of the count.
    """

    def __init__(self, sizeof: int, size: int) -> None:
        super().__init__()

        assert sizeof > 0
        assert size >= 0

        self.countx = 0
        self.sizeof = sizeof
        self.size = size
        self.maxpos = sizeof * size
        self._data = bytearray(self.maxpos)


    def __getitem__(self, idx: int) -> bytes:
        if idx >= self.size:
            raise IndexError(f"{idx} >= {self.size}")

        startpos = self.sizeof * idx
        return self._data[startpos : startpos + self.sizeof]


    def __setitem__(self, idx: int, value: bytes) -> None:
        """Set item at 'idx'

        Please note that the length of 'value' can be anything. The bytes
        are simply copied to the respective position, as long as 'value'
        fits into the remaining memory of the underlying bytearray.^

        'count' will be adjusted, if 'idx' > current count.
        """
        if idx >= self.size:
            raise IndexError(f"{idx} >= {self.size}")

        startpos = self.sizeof * idx
        endpos = startpos + len(value)
        if endpos > self.maxpos:
            raise ValueError(f"Value exceeds buffer boundary: {endpos} > {self.maxpos}")

        self._data[startpos : endpos] = value

        if idx >= self.countx:
            self.countx = idx + 1


    def append(self, value: bytes) -> None:
        if self.countx >= self.size:
            raise IndexError(f"{self.countx} >= {self.size}")

        self[self.countx] = value


    def extend(self, values: Iterable[bytes]) -> None:
        for data in values:
            self.append(data)


    def __len__(self) -> int:
        # TODO Does get() or set() use len() to validate the boundary?
        return self.countx


    def __iter__(self) -> Iterator[bytes]:
        startpos = 0
        endpos = self.sizeof * self.countx
        while startpos < endpos:
            nextpos = startpos + self.sizeof
            yield self._data[startpos : nextpos]
            startpos = nextpos


    def find(self, value: bytes) -> int:
        """Find a specific value in the array.

        Note that always the number of bytes in 'value' are compared,
        irrespective whether 'value' is shorter or longer then the 'sizeof',
        the with the constructure provided length of each value.
        """
        pos = -1
        while True:
            pos = self._data.find(value, pos + 1)
            if pos < 0:
                return -1

            if pos == 0:
                return 0

            idx, rest = divmod(pos, self.sizeof)
            if rest == 0:
                return idx


    def __contains__(self, value: bytes) -> bool:
        """False, if find() returns -1, else True"""
        return self.find(value) >= 0


    def __repr__(self) -> str:
        return "FWFList:[" + ", ".join((str(bytes(v)) for v in self)) + "]"
