---
title: Querying
description: get, get_by_key, find, find_all, and nearby.
---

All query methods are async classmethods. They return model instances, not raw Tile38 responses.

## get

`Model.get(identifier, **groups)` fetches a single object by identifier. Pass a value for each `Group()` field the model declares.

```python
truck = await Truck.get(id=1, group="fleet1")
```

`get()` raises `NotFoundError` if the object does not exist.

## get_by_key

`Model.get_by_key(identifier, key)` fetches a single object using a raw Tile38 key instead of separate group keyword arguments. Use it when you already have a key string, for example one read back from Tile38.

```python
truck = await Truck.get_by_key("1", "truck:group=fleet1")
```

## find

`Model.find(**kwargs)` is an async generator. It yields every object that matches the given groups and data-field filters.

Pass a value for each `Group()` field to scope the search to one key. Omit the groups to scan every key for the model.

```python
async for truck in Truck.find(group="fleet1"):
    print(truck.id)
```

Pass data-field values to filter by equality. TileORM combines multiple filters with AND.

```python
async for truck in Truck.find(group="fleet1", status="active"):
    print(truck.id)
```

`find()` accepts `limit` and `cursor` keyword arguments to page through results.

```python
async for truck in Truck.find(group="fleet1", limit=10):
    print(truck.id)
```

A model can declare more than one `Group()` field. Pass either all of them or none of them; `find()` raises `TypeError` if you pass some but not all.

## find_all

`Model.find_all(**kwargs)` takes the same arguments as `find()` and returns a `list` instead of an async generator.

```python
trucks = await Truck.find_all(group="fleet1", limit=10)
```

## nearby

`Model.nearby(target, radius=1000.0, **groups)` is an async generator. It yields every object within `radius` meters of `target`.

`target` accepts three forms:

- A `Point`, to search near a coordinate.
- A `Model` instance, to search near another object's location.
- A `str` object ID, to search near an existing object by ID.

```python
from tileorm import Point

async for truck in Truck.nearby(
    Point(lat=52.25, lon=13.37), radius=5000, group="fleet1"
):
    print(truck.id)
```

```python
reference = await Truck.get(id=1, group="fleet1")

async for truck in Truck.nearby(reference, radius=5000, group="fleet1"):
    print(truck.id)
```

If the target key or reference object does not exist, `nearby()` yields no results instead of raising an error.

`nearby()` only supports models with a `PointField()` location.
