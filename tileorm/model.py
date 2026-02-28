from __future__ import annotations

from typing import (
    Any,
    AsyncIterator,
    ClassVar,
    Protocol,
    Self,
    TypeVar,
    overload,
)

import pygeohash
from pydantic import BaseModel
from pydantic_core import to_json
from pyle38 import Tile38
from pyle38.errors import Tile38Error, Tile38IdNotFoundError, Tile38KeyNotFoundError
from pyle38.follower import Follower

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
    Tile38FieldInfo,
    _Location,
)
from tileorm.types import Bounds, Point

Tile38ModelType = TypeVar("Tile38ModelType", bound="Model")


class ObjectResponse(Protocol):
    """Protocol for a Tile38 object response (e.g. query.asObject() result or item in asObjects().objects)."""

    object: dict
    fields: dict[str, Any] | None


def _coordinates_to_geohash(coords: list[float], precision: int = 9) -> str:
    """Encode GeoJSON Point coordinates [lon, lat] to a geohash string."""
    lon, lat = coords[0], coords[1]
    return pygeohash.encode(lat, lon, precision=precision)


def _build_where_expr(filters: dict[str, Any]) -> str:
    """Build a Tile38 WHERE expression for equality on the given field=value pairs."""
    parts = []
    for field, value in filters.items():
        if value is None:
            literal = "null"
        elif isinstance(value, bool):
            literal = "true" if value else "false"
        elif isinstance(value, (int, float)):
            literal = str(value)
        else:
            s = str(value).replace("\\", "\\\\").replace("'", "\\'")
            literal = f"'{s}'"
        parts.append(f"{field} === {literal}")
    return " && ".join(parts)


class Model(BaseModel):
    model_config = {"coerce_numbers_to_str": True}

    class Meta:
        database: Tile38 | None = None

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
    _read_db: ClassVar[Tile38 | Follower]

    @staticmethod
    def fields_of_type(
        obj: type[Model], field_type: type[Tile38FieldInfo]
    ) -> list[str]:
        return [
            name
            for name, field in obj.model_fields.items()
            if isinstance(field, field_type)
        ]

    @classproperty
    def __identifier(cls: type[Model]) -> str:
        fields = cls.fields_of_type(cls, Identifier)

        if not fields:
            raise NoIdentifier(name=cls.__name__)

        if len(fields) > 1:
            raise MultipleIdentifiers(name=cls.__name__)

        return fields[0]

    @classproperty
    def __location(cls: type[Model]) -> str:
        fields = cls.fields_of_type(cls, _Location)

        if not fields:
            raise NoLocation(name=cls.__name__)

        if len(fields) > 1:
            raise MultipleLocations(name=cls.__name__)

        return fields[0]

    @classproperty
    def __groups(cls: type[Model]) -> list[str]:
        return cls.fields_of_type(cls, Group)

    @classproperty
    def __fields(cls: type[Model]) -> list[str]:
        return cls.fields_of_type(cls, Data)

    @classproperty
    def __json(cls: type[Model]) -> list[str]:
        return cls.fields_of_type(cls, JsonField)

    @classmethod
    def _make_key(cls, **groups: str) -> str:
        """Make a Tile38 key for the given groups.
        
        Groups are sorted alphabetically and joined with ":" and "=".

        Example:
        >>> Model._make_key(group1="value1", group2="value2")
        "model:group1=value1:group2=value2"
        """
        return (
            f"{cls.__name__.lower()}"
            f"{':' if groups else ''}"
            f"{':'.join([f"{group}={groups.get(group)}" for group in sorted(groups)])}"
        )

    @classmethod
    def _make_groups(cls, key: str) -> dict[str, str]:
        """Make a dictionary of groups from a Tile38 key.
        
        Groups are extracted from the key and joined with ":" and "=".

        Example:
        >>> Model._make_groups("model:group1=value1:group2=value2")
        {"group1": "value1", "group2": "value2"}
        """
        parts = key.split(":")

        parsed = {
            name: value for name, value in [part.split("=") for part in parts[1:]]
        }

        return {group: parsed[group] for group in cls.__groups}

    @classproperty
    def _read_db(cls: type[Model]) -> Tile38 | Follower:
        """Return the database client to use for reads: follower if available, else main."""
        database = cls.Meta.database
        if database is None:
            raise RuntimeError("Model.Meta.database must be set")
        try:
            return database.follower()
        except Tile38Error:
            return database

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

    async def save(self) -> Self:
        database = self.Meta.database
        if database is None:
            raise RuntimeError("Model.Meta.database must be set")
        query = database.set(self._key, self._identifier)

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
            await database.jset(
                self._key,
                self._identifier,
                field,
                to_json(getattr(self, field)).decode("utf-8"),
                mode="RAW",
            )
        return self

    @classmethod
    async def exists(
        cls: type[Tile38ModelType],
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
        except Tile38KeyNotFoundError:
            return False

    @classmethod
    async def create(cls: type[Tile38ModelType], **kwargs: Any) -> Tile38ModelType:
        instance = cls(**kwargs)
        await instance.save()
        return instance

    @classmethod
    def from_pyle(
        cls: type[Tile38ModelType],
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
        elif (
            isinstance(fields, list)
            and cls.__fields
            and len(fields) == len(cls.__fields)
        ):
            # SCAN returns fields as ordered list of values; map to field names
            obj.update(dict(zip(cls.__fields, fields)))
        obj.update(**(geo or {}))

        for group, value in groups.items():
            obj[group] = value
        return cls.model_validate(obj)

    @classmethod
    async def get(
        cls: type[Tile38ModelType],
        identifier: str | int,
        **groups: str,
    ) -> Tile38ModelType:
        id_str = str(identifier)
        key = cls._make_key(**groups)
        query = cls._read_db.get(key, id_str).withfields()

        try:
            result = await query.asObject()
        except Tile38KeyNotFoundError:
            raise exceptions.NotFoundError(name=cls.__name__, key=key, id=id_str)

        return cls.from_pyle(result, id_override=id_str, **groups)

    @classmethod
    async def get_by_key(
        cls: type[Tile38ModelType],
        identifier: str,
        key: str,
    ) -> Tile38ModelType:
        return await cls.get(identifier, **cls._make_groups(key))

    @overload
    @classmethod
    async def nearby(
        cls: type[Tile38ModelType],
        point: Point,
        radius: float = 1000.0,
        **groups: str,
    ) -> AsyncIterator[Tile38ModelType]: ...

    @overload
    @classmethod
    async def nearby(
        cls: type[Tile38ModelType],
        object_id: str,
        radius: float = 1000.0,
        **groups: str,
    ) -> AsyncIterator[Tile38ModelType]: ...

    @overload
    @classmethod
    async def nearby(
        cls: type[Tile38ModelType],
        model: Model,
        radius: float = 1000.0,
        **groups: str,
    ) -> AsyncIterator[Tile38ModelType]: ...

    @classmethod
    async def nearby(
        cls: type[Tile38ModelType],
        target: Point | str | Model,
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
        except Tile38KeyNotFoundError:
            # Return empty iterator if key doesn't exist
            return
        except Tile38IdNotFoundError:
            # Return empty iterator if reference object doesn't exist
            return

        for item in result.objects:
            yield cls.from_pyle(item, **groups)

    @classmethod
    async def find(
        cls: type[Tile38ModelType],
        *,
        limit: int | None = None,
        cursor: int = 0,
        **kwargs: Any,
    ) -> AsyncIterator[Tile38ModelType]:
        """Find objects that match the given filters.

        Groups (e.g. group=...) are optional. If omitted, all keys for this
        model are scanned (e.g. all fleets). If provided, only that key is
        queried. Optional equality filters on Data fields use Tile38 WHERE_EXPR.

        Yields model instances. Use with: async for obj in Model.find() or
        Model.find(group="fleet1", name="truck1").
        """
        # Split kwargs into groups (optional) and filters (Data field equality)
        groups = {k: kwargs[k] for k in cls.__groups if k in kwargs}
        if groups and len(groups) != len(cls.__groups):
            missing = set(cls.__groups) - set(groups.keys())
            raise TypeError(
                f"find() missing required group argument(s): {sorted(missing)}"
            )
        filters = {k: kwargs[k] for k in cls.__fields if k in kwargs}
        unknown = set(kwargs.keys()) - set(cls.__groups) - set(cls.__fields)
        if unknown:
            raise TypeError(
                f"find() got unexpected keyword argument(s): {sorted(unknown)}"
            )

        db = cls._read_db
        keys_to_scan: list[str]
        if len(groups) == len(cls.__groups):
            keys_to_scan = [cls._make_key(**groups)]
        else:
            # No (or partial) groups: scan all keys for this model
            keys_response = await db.keys(f"{cls.__name__.lower()}*")
            keys_to_scan = keys_response.keys or []

        yielded = 0
        for key in keys_to_scan:
            key_groups = cls._make_groups(key)
            query = db.scan(key)

            if limit is not None:
                if remaining := limit - yielded <= 0:
                    return
                query = query.limit(remaining)

            if cursor != 0:
                query = query.cursor(cursor)

            if filters:
                expr = _build_where_expr(filters)
                query = query.where_expr(expr)

            try:
                result = await query.asObjects()
            except Tile38KeyNotFoundError:
                continue

            for item in result.objects:
                yield cls.from_pyle(item, **key_groups)
                yielded += 1
                if limit is not None and yielded >= limit:
                    return
            # Only apply cursor to the first key when scanning multiple keys
            cursor = 0

    @classmethod
    async def find_all(
        cls: type[Tile38ModelType],
        *,
        limit: int | None = None,
        cursor: int = 0,
        **kwargs: Any,
    ) -> list[Tile38ModelType]:
        """Return a list of all objects matching the find criteria by consuming the find iterator."""
        return [obj async for obj in cls.find(limit=limit, cursor=cursor, **kwargs)]
