---
title: tileorm.fields
description: Field functions used to declare a Model's attributes.
---

Each function below returns a Pydantic field marker. Use it as the default value of a model attribute, the same way you'd use Pydantic's `Field()`.

| Function | Role |
| --- | --- |
| `Identifier(**kwargs)` | The object's ID. Exactly one per model. |
| `Group(**kwargs)` | Splits a model's objects across Tile38 keys. Zero or more per model. |
| `PointField(**kwargs)` | Location stored as a `Point`. One location field per model. |
| `BoundsField(**kwargs)` | Location stored as a `Bounds`. One location field per model. |
| `GeoHashField(**kwargs)` | Location stored as a geohash string. One location field per model. |
| `CharField(**kwargs)` | A `str` data field. |
| `FloatField(**kwargs)` | A `float` data field. |
| `IntegerField(**kwargs)` | An `int` data field. |
| `JsonField(**kwargs)` | A JSON-serializable data field. |

All functions accept the keyword arguments Pydantic's `Field()` accepts, for example `default`.

See [Defining models](/guides/models/) for how each field type maps to Tile38.
