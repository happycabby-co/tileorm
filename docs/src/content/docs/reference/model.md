---
title: tileorm.model
description: The Model base class.
---

## Model

`Model` is a Pydantic `BaseModel`. Subclass it and add TileORM fields to define a Tile38-backed model. See [Defining models](/guides/models/) for field types and [Meta.database](/guides/models/#metadatabase).

### Class methods

| Method | Description |
| --- | --- |
| `create(**kwargs)` | Build and save a new instance. Returns the instance. |
| `get(identifier, **groups)` | Fetch one object by identifier. Raises `NotFoundError` if missing. |
| `get_by_key(identifier, key)` | Fetch one object using a raw Tile38 key instead of group keyword arguments. |
| `find(*, limit=None, cursor=0, **kwargs)` | Async generator yielding objects matching groups and data-field filters. |
| `find_all(*, limit=None, cursor=0, **kwargs)` | Same as `find()`, returned as a `list`. |
| `nearby(target, radius=1000.0, **groups)` | Async generator yielding objects within `radius` meters of `target`. |
| `exists(identifier, **groups)` | Return `True` or `False` without fetching the full object. |

### Instance methods

| Method | Description |
| --- | --- |
| `save()` | Write the instance's current field values to Tile38. Returns `self`. |
| `delete()` | Remove the instance from Tile38. |

See [Querying](/guides/querying/) and [Saving and deleting](/guides/instance-methods/) for usage examples.
