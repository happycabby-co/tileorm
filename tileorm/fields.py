from pydantic import fields


class Tile38FieldInfo(fields.FieldInfo):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


class Identifier(Tile38FieldInfo): ...


class _Location(Tile38FieldInfo): ...


class PointField(_Location): ...


class BoundsField(_Location): ...


class GeoHashField(_Location): ...


class Group(Tile38FieldInfo): ...


class JsonField(Tile38FieldInfo): ...


class Data(Tile38FieldInfo): ...


class CharField(Data): ...


class FloatField(Data): ...


class IntegerField(Data): ...


class ComplexField(Data): ...
