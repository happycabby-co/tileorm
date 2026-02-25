from typing import (
    Any,
    AsyncIterator,
    ClassVar,
    Dict,
    Iterable,
    Optional,
    overload,
    Protocol,
    Type,
    TypeVar,
    Union,
)

import pygeohash
import pyle38
import pyle38.errors
from pydantic import BaseModel
from pydantic_core import to_json
from pyle38 import Tile38, errors

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
    PointField,
    _Location,
)
from tileorm.types import Bounds, Point

Tile38ModelType = TypeVar("Tile38ModelType", bound="Model")


class ObjectResponse(Protocol):
    """Protocol for a Tile38 object response (e.g. query.asObject() result or item in asObjects().objects)."""

    object: dict
    fields: Optional[Dict[str, Any]]


def _coordinates_to_geohash(coords: list[float], precision: int = 9) -> str:
    """Encode GeoJSON Point coordinates [lon, lat] to a geohash string."""
    lon, lat = coords[0], coords[1]
    return pygeohash.encode(lat, lon, precision=precision)


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
    _read_db: ClassVar[Tile38]

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
    def _make_key(cls, **groups: str) -> str:
        return (
            f"{cls.__name__.lower()}"
            f"{':' if groups else ''}"
            f"{':'.join([f"{group}={groups.get(group)}" for group in sorted(groups)])}"
        )

    @classmethod
    def _make_groups(cls, key: str) -> dict[str, str]:
        return {
            group: key.split(":")[i + 1].split("=")[1]
            for i, group in enumerate(cls.__groups)
        }

    @classproperty
    def _read_db(cls) -> Tile38:
        """Return the database client to use for reads: follower if available, else main."""
        try:
            return cls.Meta.database.follower()
        except errors.Tile38Error:
            return cls.Meta.database

    @property
    def _key(self) -> str:
        return self._make_key(**self._groups)

    @property
    def _groups(self) -> dict[str, str]:
        return {group: getattr(self, group) for group in self.__groups}

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
                await cls._read_db.exists(
                    cls._make_key(**groups),
                    identifier,
                )
            ).exists
        except errors.Tile38KeyNotFoundError:
            return False

    @classmethod
    async def create(cls: Type[Tile38ModelType], **kwargs) -> Tile38ModelType:
        return await cls(**kwargs).save()

    @classmethod
    def from_pyle(
        cls: Type[Tile38ModelType],
        response: ObjectResponse,
        *,
        id_override: str | None = None,
        **groups: str,
    ) -> Tile38ModelType:
        """Build a model instance from a pyle38 object response (e.g. query.asObject() or item in asObjects().objects).
        For GET, the single-object response does not include an id attribute, so pass id_override.
        """
        identifier = (
            id_override if id_override is not None else getattr(response, "id", None)
        )
        if identifier is None:
            raise ValueError(
                "response must have an id attribute or id_override must be passed"
            )
        geo = response.object

        obj = {cls.__identifier: identifier}

        match cls.model_fields[cls.__location]:
            case PointField():
                obj[cls.__location] = Point(
                    geo["coordinates"][1],
                    geo["coordinates"][0],
                )
            case GeoHashField():
                obj[cls.__location] = _coordinates_to_geohash(
                    geo["coordinates"], precision=9
                )
            case BoundsField():
                obj[cls.__location] = Bounds(
                    geo["coordinates"][0][0][1],
                    geo["coordinates"][0][0][0],
                    geo["coordinates"][0][2][1],
                    geo["coordinates"][0][2][0],
                )
            case _:
                raise NotImplementedError

        fields = getattr(response, "fields", None)
        if isinstance(fields, dict):
            obj.update(fields)
        obj.update(**(geo or {}))

        for group, value in groups.items():
            obj[group] = value
        return cls.model_validate(obj)

    @classmethod
    async def get(
        cls: Type[Tile38ModelType],
        identifier: str,
        **groups: str,
    ) -> Tile38ModelType:
        key = cls._make_key(**groups)
        query = cls._read_db.get(key, identifier).withfields()

        try:
            result = await query.asObject()
        except pyle38.errors.Tile38KeyNotFoundError:
            raise exceptions.NotFoundError(name=cls.__name__, key=key, id=identifier)

        return cls.from_pyle(result, id_override=identifier, **groups)

    @classmethod
    async def get_by_key(
        cls: Type[Tile38ModelType],
        identifier: str,
        key: str,
    ) -> Tile38ModelType:
        return await cls.get(identifier, **cls._make_groups(key))

    @overload
    @classmethod
    async def nearby(
        cls: Type[Tile38ModelType],
        point: Point,
        radius: float = 1000.0,
        **groups: str,
    ) -> AsyncIterator[Tile38ModelType]: ...

    @overload
    @classmethod
    async def nearby(
        cls: Type[Tile38ModelType],
        object_id: str,
        radius: float = 1000.0,
        **groups: str,
    ) -> AsyncIterator[Tile38ModelType]: ...

    @overload
    @classmethod
    async def nearby(
        cls: Type[Tile38ModelType],
        model: "Model",
        radius: float = 1000.0,
        **groups: str,
    ) -> AsyncIterator[Tile38ModelType]: ...

    @classmethod
    async def nearby(
        cls: Type[Tile38ModelType],
        target: Union[Point, str, "Model"],
        radius: float = 1000.0,
        **groups: str,
    ) -> AsyncIterator[Tile38ModelType]:
        key = cls._make_key(**groups)
        db = cls._read_db
        query = db.nearby(key)

        # Determine if target is a Point, object_id (str), or Model instance
        if isinstance(target, Point):
            # Query near a point - radius can be passed as third parameter to point()
            query = query.point(target.lat, target.lon, int(radius))
        elif isinstance(target, str):
            # Query near another object by ID - need to get the object first to get its location
            try:
                ref_obj = await cls.get(target, **groups)
            except exceptions.NotFoundError:
                # Return empty iterator if reference object doesn't exist
                return
            if isinstance(ref_obj._location, Point):
                query = query.point(
                    ref_obj._location.lat, ref_obj._location.lon, int(radius)
                )
            else:
                raise NotImplementedError(
                    f"nearby queries with object_id only support Point locations, got {type(ref_obj._location)}"
                )
        elif isinstance(target, Model):
            # Query near a Model instance - use its location
            if isinstance(target._location, Point):
                query = query.point(
                    target._location.lat, target._location.lon, int(radius)
                )
            else:
                raise NotImplementedError(
                    f"nearby queries with Model only support Point locations, got {type(target._location)}"
                )
        else:
            raise TypeError(
                f"target must be a Point, str (object_id), or Model instance, got {type(target)}"
            )

        # Fields are returned by default in pyle38 if they exist
        # For nearby queries, fields should be included automatically if objects have fields

        try:
            result = await query.asObjects()
        except pyle38.errors.Tile38KeyNotFoundError:
            # Return empty iterator if key doesn't exist
            return
        except pyle38.errors.Tile38ObjectNotFoundError:
            # Return empty iterator if reference object doesn't exist
            return

        for item in result.objects:
            yield cls.from_pyle(item, **groups)
