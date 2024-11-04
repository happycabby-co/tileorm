from typing import Any, ClassVar, Iterable, Type, TypeVar

from pydantic import BaseModel
from pydantic_core import to_json
from pyle38 import Tile38, errors
import pyle38
import pyle38.errors

from tileorm import exceptions
from tileorm.exceptions import (
    MultipleIdentifiers,
    MultipleLocations,
    NoIdentifier,
    NoLocation,
)
from tileorm.fields import (
    BoundsField,
    Data,
    GeoHashField,
    Group,
    Identifier,
    JsonField,
    _Location,
    PointField,
)
from tileorm.types import Bounds, Point


Tile38ModelType = TypeVar("Tile38ModelType", bound="Model")


class Model(BaseModel):
    model_config = {"coerce_numbers_to_str": True}

    class Meta:
        database: Tile38 = None

    def __init__(self, **data: Any) -> None:
        super().__init__(**data)
        self._identifier
        self._location

    class classproperty:
        def __init__(self, func):
            self.fget = func

        def __get__(self, instance, owner):
            return self.fget(owner)

    __identifier: ClassVar[str]
    __location: ClassVar[str]
    __groups: ClassVar[list[str]]
    __fields: ClassVar[list[str]]
    __json: ClassVar[list[str]]

    @staticmethod
    def fields_of_type(obj: "Type[Model]", field_types: list[type]) -> list[str]:
        if not isinstance(field_types, Iterable):
            field_types = [field_types]

        return [
            name
            for name, field in obj.model_fields.items()
            if any(isinstance(field, field_type) for field_type in field_types)
        ]

    @classproperty
    def __identifier(cls) -> str:
        fields = cls.fields_of_type(cls, Identifier)

        if not fields:
            raise NoIdentifier(name=cls.__name__)

        if len(fields) > 1:
            raise MultipleIdentifiers(name=cls.__name__)

        return fields[0]

    @classproperty
    def __location(cls) -> str:
        fields = cls.fields_of_type(cls, _Location)

        if not fields:
            raise NoLocation(name=cls.__name__)

        if len(fields) > 1:
            raise MultipleLocations(name=cls.__name__)

        return fields[0]

    @classproperty
    def __groups(cls) -> list[str]:
        return cls.fields_of_type(cls, Group)

    @classproperty
    def __fields(cls) -> list[str]:
        return cls.fields_of_type(cls, Data)

    @classproperty
    def __json(cls) -> list[str]:
        return cls.fields_of_type(cls, JsonField)

    @classmethod
    def __make_key(cls, **groups: str) -> str:
        return (
            f"{cls.__name__.lower()}"
            f"{':' if groups else ''}"
            f"{':'.join([str(groups.get(group)) for group in sorted(groups)])}"
        )

    @property
    def _key(self) -> str:
        return self.__make_key(
            **{group: getattr(self, group) for group in self.__groups}
        )

    @property
    def _identifier(self):
        return getattr(self, self.__identifier)

    @property
    def _location(self):
        return getattr(self, self.__location)

    async def save(self) -> None:
        query = self.Meta.database.set(self._key, self._identifier)

        match self.model_fields[self.__location]:
            case PointField():
                query = query.point(self._location.lat, self._location.lon)
            case GeoHashField():
                query = query.hash(self._location)
            case BoundsField():
                query = query.bounds(*self._location)
            case _:
                raise NotImplementedError

        if self.__fields:
            query = query.fields(
                {field: to_json(getattr(self, field)) for field in self.__fields}
            )

        await query.exec()

        if not self.__json:
            return self

        for field in self.__json:
            await self.Meta.database.jset(
                self._key,
                self._identifier,
                field,
                to_json(getattr(self, field)).decode("utf-8"),
                mode="RAW",
            )
        return self

    @classmethod
    async def exists(
        cls: Type[Tile38ModelType],
        identifier: str,
        **groups: str,
    ) -> bool:
        try:
            return (
                await cls.Meta.database.follower().exists(
                    cls.__make_key(**groups),
                    identifier,
                )
            ).exists
        except errors.Tile38KeyNotFoundError:
            return False

    @classmethod
    async def create(cls: Type[Tile38ModelType], **kwargs) -> Tile38ModelType:
        return await cls(**kwargs).save()

    @classmethod
    async def get(
        cls: Type[Tile38ModelType],
        identifier: str,
        **groups: str,
    ) -> Tile38ModelType:
        key = cls.__make_key(**groups)

        try:
            result = (
                await cls.Meta.database.get(key, identifier).withfields().asObject()
            )
        except pyle38.errors.Tile38KeyNotFoundError:
            raise exceptions.NotFoundError(name=cls.__name__, key=key, id=identifier)

        obj = {cls.__identifier: identifier}

        match cls.model_fields[cls.__location]:
            case PointField():
                obj[cls.__location] = Point(
                    result.object["coordinates"][1], result.object["coordinates"][0]
                )
            case GeoHashField():
                obj[cls.__location] = result.hash
            case BoundsField():
                obj[cls.__location] = Bounds(
                    result.object["coordinates"][0][0][1],
                    result.object["coordinates"][0][0][0],
                    result.object["coordinates"][0][2][1],
                    result.object["coordinates"][0][2][0],
                )
            case _:
                raise NotImplementedError

        obj.update(**(result.fields or {}), **(result.object or {}))

        for group, value in groups.items():
            obj[group] = value
        return cls.model_validate(obj)
