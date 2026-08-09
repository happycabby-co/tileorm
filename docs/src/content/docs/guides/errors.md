---
title: Error handling
description: TileOrmException and its subclasses.
---

Every exception TileORM raises deliberately is a subclass of `TileOrmException`. Catch `TileOrmException` to handle any of them in one place.

```python
from tileorm import TileOrmException

try:
    truck = await Truck.get(id=1, group="fleet1")
except TileOrmException as exc:
    print(exc)
```

## Model definition errors

TileORM checks a model's fields when you first instantiate it, not when you declare the class.

- `NoIdentifier` — the model has no `Identifier()` field.
- `MultipleIdentifiers` — the model has more than one `Identifier()` field.
- `NoLocation` — the model has no location field (`PointField()`, `BoundsField()`, or `GeoHashField()`).
- `MultipleLocations` — the model has more than one location field.

Fix these by changing the model definition. They signal a bug in the model, not bad input data.

## Query errors

- `NotFoundError` — `Model.get()` or `Model.get_by_key()` found no object with the given identifier and key.

```python
from tileorm import NotFoundError

try:
    truck = await Truck.get(id=999, group="fleet1")
except NotFoundError:
    truck = None
```

`find()` and `nearby()` do not raise `NotFoundError`. A search with no matches yields an empty result instead.
