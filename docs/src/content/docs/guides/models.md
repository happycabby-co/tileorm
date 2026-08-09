---
title: Defining models
description: Identifier, Group, and data fields, and how they map to Tile38.
---

A TileORM model is a Pydantic model. Each field uses a TileORM field function to tell TileORM what role the field plays in Tile38.

## Identifier

Every model needs exactly one `Identifier()` field. Tile38 uses this value as the object ID.

```python
from tileorm import Identifier, Model


class Truck(Model):
    id: int = Identifier()
    ...
```

A model with zero or more than one `Identifier()` field raises `NoIdentifier` or `MultipleIdentifiers` when you instantiate it. See [Error handling](/guides/errors/).

## Location

Every model also needs exactly one location field. Choose one of:

- `PointField()` — stores a `Point(lat, lon)`.
- `BoundsField()` — stores a `Bounds(minlat, minlon, maxlat, maxlon)`.
- `GeoHashField()` — stores a geohash string.

```python
from tileorm import Model, Point, PointField


class Truck(Model):
    location: Point = PointField()
    ...
```

See [Geo types](/guides/geo-types/) for the full shape of `Point` and `Bounds`.

## Group

`Group()` fields split a model's objects across separate Tile38 keys. Use groups to scope objects, for example by fleet, region, or tenant.

```python
from tileorm import Group, Model


class Truck(Model):
    group: str = Group()
    ...
```

TileORM builds the Tile38 key from the model name and its group values, for example `truck:group=fleet1`. A model can declare more than one `Group()` field; TileORM sorts the group names alphabetically when it builds the key.

## Data fields

Data fields store plain values alongside the location. TileORM saves them as Tile38 fields on the object.

| Field            | Python type                 |
| ---------------- | --------------------------- |
| `CharField()`    | `str`                       |
| `FloatField()`   | `float`                     |
| `IntegerField()` | `int`                       |
| `JsonField()`    | any JSON-serializable value |

```python
from tileorm import CharField, FloatField, IntegerField, JsonField, Model


class Truck(Model):
    name: str = CharField()
    speed: float = FloatField()
    passengers: int = IntegerField()
    metadata: dict = JsonField()
```

Data fields accept normal Pydantic field arguments, for example a default value:

```python
name: str | None = CharField(default=None)
```

## Meta.database

Set `Meta.database` to the `Tile38` client the model should use for reads and writes.

```python
from tileorm import Model, Tile38

db = Tile38("redis://localhost:9851")


class Truck(Model):
    class Meta:
        database = db
```
