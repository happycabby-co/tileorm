---
title: tileorm.exceptions
description: TileOrmException and its subclasses.
---

All exceptions inherit from `TileOrmException`, which inherits from `Exception`.

| Exception | Raised when |
| --- | --- |
| `NoIdentifier` | A model has no `Identifier()` field. |
| `MultipleIdentifiers` | A model has more than one `Identifier()` field. |
| `NoLocation` | A model has no location field. |
| `MultipleLocations` | A model has more than one location field. |
| `NotFoundError` | `get()` or `get_by_key()` found no matching object. |

See [Error handling](/guides/errors/) for usage examples.
