---
title: Saving and deleting
description: create, save, delete, and exists.
---

## create

`Model.create(**kwargs)` builds a new model instance and saves it in one step. It returns the saved instance.

```python
truck = await Truck.create(
    id=1,
    group="fleet1",
    location=Point(lat=52.25, lon=13.37),
    name="truck1",
)
```

## save

`instance.save()` writes the current field values to Tile38. Call it after you change a field on an instance you built yourself, or after you build one with `Model(...)`.

```python
truck = Truck(id=1, group="fleet1", location=Point(lat=52.25, lon=13.37), name="truck1")
await truck.save()
```

`save()` returns the same instance, so you can chain it:

```python
truck = await Truck(
    id=1, group="fleet1", location=Point(lat=52.25, lon=13.37), name="truck1"
).save()
```

## delete

`instance.delete()` removes the object from Tile38.

```python
await truck.delete()
```

## exists

`Model.exists(identifier, **groups)` returns `True` or `False` without fetching the full object.

```python
if await Truck.exists(id=1, group="fleet1"):
    print("truck1 is on the map")
```

Both `save()` and `delete()` need `Meta.database` to be set. Calling either without a database configured raises `RuntimeError`.
