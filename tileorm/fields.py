from typing import Any

from pydantic import fields


class Tile38FieldInfo(fields.FieldInfo):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


class _Identifier(Tile38FieldInfo): ...


class _Location(Tile38FieldInfo): ...


class _PointField(_Location): ...


class _BoundsField(_Location): ...


class _GeoHashField(_Location): ...


class _Group(Tile38FieldInfo): ...


class _JsonField(Tile38FieldInfo): ...


class _Data(Tile38FieldInfo): ...


class _CharField(_Data): ...


class _FloatField(_Data): ...


class _IntegerField(_Data): ...


class _ComplexField(_Data): ...


# Factory functions returning Any so that "name: T = FieldName()" type-checks.
def Identifier(**kwargs: Any) -> Any:
    return _Identifier(**kwargs)


def Group(**kwargs: Any) -> Any:
    return _Group(**kwargs)


def PointField(**kwargs: Any) -> Any:
    return _PointField(**kwargs)


def BoundsField(**kwargs: Any) -> Any:
    return _BoundsField(**kwargs)


def GeoHashField(**kwargs: Any) -> Any:
    return _GeoHashField(**kwargs)


def JsonField(**kwargs: Any) -> Any:
    return _JsonField(**kwargs)


def CharField(**kwargs: Any) -> Any:
    return _CharField(**kwargs)


def FloatField(**kwargs: Any) -> Any:
    return _FloatField(**kwargs)


def IntegerField(**kwargs: Any) -> Any:
    return _IntegerField(**kwargs)


def ComplexField(**kwargs: Any) -> Any:
    return _ComplexField(**kwargs)


# Data is used in fields_of_type, not as a default in model bodies; alias for compatibility.
Data = _Data
