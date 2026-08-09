---
title: Getting started
description: Install TileORM, connect to Tile38, and create your first object.
---

TileORM connects a Pydantic model to a collection in [Tile38](https://tile38.com). You define the model. TileORM builds the Tile38 commands.

## Install

Install TileORM with pip:

```shell
pip install tileorm
```

TileORM needs a running Tile38 server. See the [Tile38 documentation](https://tile38.com/topics/install) for install steps.

## Connect to Tile38

Create a `Tile38` client. Point it at your server.

```python
from tileorm import Tile38

db = Tile38("redis://localhost:9851")
```

## Define a model

A model is a Pydantic model with TileORM fields. Every model needs one `Identifier` field and one location field (`PointField`, `BoundsField`, or `GeoHashField`).

```python
from tileorm import CharField, Group, Identifier, Model, PointField


class Truck(Model):
    id: int = Identifier()
    group: str = Group()
    field: str = CharField()

    class Meta:
        database = db
```

The `Meta.database` attribute tells the model which `Tile38` client to use.

## Create an object

Call `Model.create()` with a value for each field, plus a `location`. `create()` saves the object to Tile38 and returns the model instance.

```python
from tileorm import Point

truck1 = await Truck.create(
    id=1,
    group="fleet1",
    location=Point(lat=52.25, lon=13.37),
    field="value",
)
```

## Get an object back

Call `Model.get()` with the identifier and any group values. `get()` returns a model instance built from the stored object.

```python
truck = await Truck.get(id=1, group="fleet1")
# Truck(id=1, location=Point(lat=52.25, lon=13.37), group='fleet1', field='value')
```

## Next steps

- [Defining models](/guides/models/) covers every field type.
- [Querying](/guides/querying/) covers `get`, `find`, and `nearby`.
