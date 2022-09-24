#!/usr/bin/env python
# encoding: utf-8


"""Define two classes. One to hold a single field specification,
and one to hold all field specifications which define a file."""

from typing import Any, Optional, Iterator
from collections import OrderedDict


class FWFFieldSpec:
    """The specification of a single field"""

    def __init__(self, startpos: int, name: str, **kwargs):
        assert len(name) > 0

        self.name = name
        self.fslice = slice(0, 0)
        self.flen = 0
        self.set_pos(startpos=startpos, **kwargs)
        self.attr = kwargs


    def _validate(self) -> None:
        assert 0 <= self.len < 1000
        assert self.fslice.start >= 0
        assert self.fslice.stop >= self.fslice.start


    def __contains__(self, attr) -> bool:
        return self.get(attr, None) is not None


    def __getitem__(self, attr: str) -> Any:
        rtn = self.get(attr)
        if rtn is not None:
            return rtn

        raise AttributeError(f"Fieldspec('{self.name}'): Attribute '{attr}' not found in {self.attr}")


    def __getattr__(self, attr: str) -> Any:
        return self[attr]


    def get(self, attr: str, default=None) -> None|Any:
        """Get the attribute"""

        if attr == "start":
            return self.fslice.start
        if attr == "stop":
            return self.fslice.stop

        return self.attr.get(attr, default)


    def set_pos(self, startpos=0, **kwargs) -> None:
        """Update the start and stop position of the field and a combination
        of 'slice', 'len', 'start' and 'stop' attributes"""

        fslice = kwargs.get("slice", None)
        flen = kwargs.get("len", None)
        start = kwargs.get("start", None)
        stop = kwargs.get("stop", None)

        if fslice is not None:
            if start is not None or stop is not None or flen is not None:
                raise KeyError(f"If 'slice' is present, 'start', 'stop' or 'len' are not allowed: {kwargs.keys()}")

            if isinstance(fslice, slice):
                start = fslice.start
                stop = fslice.stop
            elif isinstance(fslice, tuple) and len(fslice) == 2:
                start, stop = fslice
            elif isinstance(fslice, list) and len(fslice) == 2:
                start, stop = fslice
            else:
                raise KeyError(f"Fieldspec: 'slice' must be one of slice, tuple(2), list(2)': {fslice}")
        elif start is not None and flen is not None:
            if stop is not None:
                raise KeyError(f"If 'start' and 'len' are present, 'stop' is not allowed: {kwargs.keys()}")

            stop = start + flen
        elif stop is not None and flen is not None:
            if start is not None:
                raise KeyError(f"If 'stop' and 'len' are present, 'start' is not allowed: {kwargs.keys()}")

            start = stop - flen
        elif start is not None and stop is not None:
            if flen is not None:
                raise KeyError(f"If 'start' and 'stop' are present, 'len' is not allowed: {kwargs.keys()}")

        elif flen is not None:
            start = startpos
            stop = start + flen
        else:
            raise KeyError(
                f"Fieldspecs requires either 'len', 'slice', 'start' or 'stop' combinations: {kwargs.keys()}")

        try:
            start = int(start)
        except ValueError as exc:
            raise ValueError(f"'start' is not a valid integer: '{start}'") from exc

        try:
            stop = int(stop)
        except ValueError as exc:
            raise ValueError(f"'stop' is not a valid integer: '{stop}'") from exc

        self.fslice = slice(start, stop)
        self.len = self.fslice.stop - self.fslice.start
        self._validate()


    def __str__(self) -> str:
        """Compute the “informal” string representation of an object

        A representation that is useful for printing the object
        """
        return f"{self.name}=({self.fslice.start}, {self.fslice.stop})"


    def __repr__(self) -> str:
        """Compute the “official” string representation of an object

        A representation that has all information about the object
        """
        return f"{self.__class__.__name__}(name=\"{self.name}\", slice=({self.fslice.start}, {self.fslice.stop}))"

# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------

class FWFFileFieldSpecs:
    """CSV is a tabular format. This class maintains the field specifications"""

    def __init__(self, specs: list[dict[str, Any]]):
        """Constructor"""

        self.fields = self._init(specs)
        self.reclen = self.record_length()


    def _init(self, specs: list[dict[str, Any]]) -> OrderedDict[str, FWFFieldSpec]:
        startpos = 0
        _fields: OrderedDict[str, FWFFieldSpec] = OrderedDict()
        for spec in specs:
            _name = spec["name"]
            if _name in _fields:
                raise KeyError(f"Names must be unique: '{_name}' in {specs}")

            field = _fields[_name] = FWFFieldSpec(startpos=startpos, **spec)
            startpos += field.len
            startpos = max(startpos, field.stop)

        return _fields


    def record_length(self) -> int:
        """Return the line length"""
        if len(self.fields) == 0:
            return 0

        return max(x["stop"] for x in self.fields.values())


    def get(self, key: str, default=None) -> Optional[FWFFieldSpec]:
        """Get a fieldspec by name or index"""
        return self.fields.get(key, default)


    def __getitem__(self, key: str) -> FWFFieldSpec:
        value = self.get(key)
        if value is not None:
            return value

        raise KeyError(f"Fieldspec does not contain field: {key}")


    def __contains__(self, key: str) -> bool:
        return key in self.fields


    def keys(self):
        """The list of all field names"""
        return self.fields.keys()


    def names(self):
        """The list of all field names. Sames as keys() but more meaningful"""
        return self.keys()


    def values(self):
        """The list of all field specifications"""
        return self.fields.values()


    def __iter__(self) -> Iterator[FWFFieldSpec]:
        for field in self.names():
            yield self[field]


    def items(self):
        """Similar to dict's items()"""
        return self.fields.items()


    def clone(self, *fields: str) -> 'FWFFileFieldSpecs':
        """Create a copy of the spec with selected and or re-ordered columns"""

        spec = FWFFileFieldSpecs([])
        new_fields = fields or self.fields.keys()
        for field in new_fields:
            spec.fields[field] = self.fields[field]

        spec.reclen = spec.record_length()
        return spec


    def add_field(self, name:str, **kwargs) -> None:
        """Add an additional field to the spec"""
        field = FWFFieldSpec(startpos=self.reclen, name=name, **kwargs)
        self.fields[name] = field
        self.reclen = self.record_length()


    def update_field(self, name:str, **kwargs) -> None:
        """Update an existing field to the spec"""
        self.fields[name].set_pos(**kwargs)
        self.reclen = self.record_length()

    def __len__(self):
        return len(self.fields)


    def __str__(self) -> str:
        return self.__repr__()


    def __repr__(self) -> str:
        fields = "[" + ", ".join(f.__str__() for f in self.fields.values()) + "]"
        return f"{self.__class__.__name__}(reclen={self.reclen}, fields={fields})"
