#!/usr/bin/env python
# encoding: utf-8


"""Define two classes. One to hold a single field specification,
and one to hold all field specifications which define a file."""

from typing import Any, MutableMapping, TypeVar, Type
from collections import OrderedDict

# CSV, FWF, Excel, etc. may all have different FieldSpecs

FieldSpec = MutableMapping[str, Any]

class FWFFieldSpec(dict[str, Any], FieldSpec):
    """A FieldSpec for fixed-width fields"""

    def __init__(self, startpos: int, name: str, **kvargs):
        assert len(name) > 0

        self.name = name
        super().__init__(name=name, **kvargs)

        self.update(startpos=startpos, **kvargs)


    def __getattr__(self, attr: str) -> Any:
        return self[attr]


    def update(self, **kvargs) -> None:
        """Update the start and stop position of the field and a combination
        of 'slice', 'len', 'start' and 'stop' attributes"""

        startpos = kvargs.pop("startpos", 0)

        fslice = kvargs.pop("slice", None)
        flen = kvargs.pop("len", None)
        start = kvargs.pop("start", None)
        stop = kvargs.pop("stop", None)

        super().update(**kvargs)

        if fslice is not None:
            if start is not None or stop is not None or flen is not None:
                raise KeyError(f"If 'slice' is present, 'start', 'stop' or 'len' are not allowed: {kvargs.keys()}")

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
                raise KeyError(f"If 'start' and 'len' are present, 'stop' is not allowed: {kvargs.keys()}")

            stop = start + flen
        elif stop is not None and flen is not None:
            if start is not None:
                raise KeyError(f"If 'stop' and 'len' are present, 'start' is not allowed: {kvargs.keys()}")

            start = stop - flen
        elif start is not None and stop is not None:
            if flen is not None:
                raise KeyError(f"If 'start' and 'stop' are present, 'len' is not allowed: {kvargs.keys()}")

        elif flen is not None:
            start = startpos
            stop = start + flen
        else:
            raise KeyError(
                f"Fieldspecs requires either 'len', 'slice', 'start' or 'stop' combinations: {kvargs.keys()}")

        try:
            start = int(start)
        except ValueError as exc:
            raise ValueError(f"'start' is not a valid integer: '{start}'") from exc

        try:
            stop = int(stop)
        except ValueError as exc:
            raise ValueError(f"'stop' is not a valid integer: '{stop}'") from exc

        self["slice"] = fslice = slice(start, stop)
        self["len"] = flen = fslice.stop - fslice.start
        self["start"] = fslice.start
        self["stop"] = fslice.stop

        assert 0 <= flen < 1000
        assert fslice.start >= 0
        assert fslice.stop >= fslice.start

# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------

class FileFieldSpecsException(Exception):
    "FileFieldSpecsException"


T = TypeVar('T', bound=FieldSpec)

class FileFieldSpecs(OrderedDict[str, T]):
    """An abstract base class for file specifications"""

    def __init__(self, fieldspec_type: Type, specs: list[dict[str, Any]]):
        """Constructor"""

        self.fieldspec_type = fieldspec_type
        super().__init__()

        for spec in specs:
            _name = spec["name"]
            if _name in self:
                raise KeyError(f"Names must be unique: '{_name}' in {specs}")

            self[_name] = self.new_field_spec(**spec)


    def new_field_spec(self, **data) -> T:
        """Create a new field spec"""
        return self.fieldspec_type(**data)


    def names(self) -> list[str]:
        """The list of all field names. Sames as keys() but more meaningful"""
        return list(self.keys())


    def clone(self, *fields: str):
        """Create a copy of the spec with selected and or re-ordered columns"""

        rtn = self.__class__(self.fieldspec_type, [])
        for field in fields:
            rtn[field] = self[field]

        return rtn


    def add_field(self, name:str, **kvargs) -> None:
        """Add an additional field to the spec"""
        field = self.new_field_spec(name=name, **kvargs)
        self[name] = field


    def update_field(self, name:str, **kvargs) -> None:
        """Update an existing field to the spec"""
        self[name].update(**kvargs)


    def apply_defaults(self, defaults: None|dict[str, Any]) -> None:
        """Apply the defaults to ALL fields"""
        if defaults:
            for spec in self.values():
                for k, data in defaults.items():
                    spec.setdefault(k, data)


    def __str__(self) -> str:
        return self.__repr__()


    def __repr__(self) -> str:
        fields = "[" + ", ".join(f.__str__() for f in self.values()) + "]"
        return f"{self.__class__.__name__}(fields={fields})"


# --------------------------------------------------------------------------------
# --------------------------------------------------------------------------------

class FWFFileFieldSpecs(FileFieldSpecs[FWFFieldSpec]):
    """A file specification for fwf file"""

    def __init__(self, specs: list[dict[str, Any]]):
        """Constructor"""

        self.startpos = 0
        super().__init__(FWFFieldSpec, specs)
        self.reclen = self.record_length()


    def new_field_spec(self, **data) -> FWFFieldSpec:
        """Create a new field spec"""
        rtn = FWFFieldSpec(startpos=self.startpos, **data)

        self.startpos += rtn.len
        self.startpos = max(self.startpos, rtn.stop)
        return rtn


    def record_length(self) -> int:
        """Return the line length"""
        if len(self) == 0:
            return 0

        return max(field["stop"] for field in self.values())


    def add_field(self, name:str, **kvargs) -> None:
        """Add an additional field to the spec"""
        super().add_field(name, **kvargs)
        self.reclen = self.record_length()


    def update_field(self, name:str, **kvargs) -> None:
        """Update an existing field to the spec"""
        super().update_field(name, **kvargs)
        self.reclen = self.record_length()


    def __repr__(self) -> str:
        fields = "[" + ", ".join(f.__str__() for f in self.values()) + "]"
        return f"{self.__class__.__name__}(reclen={self.reclen}, fields={fields})"
