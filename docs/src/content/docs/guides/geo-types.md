---
title: Geo types
description: Point, PointLike, and Bounds.
---

## Point

`Point` is a named tuple with `lat` and `lon` fields. Use it as the value of a `PointField()`.

```python
from tileorm import Point

location = Point(lat=52.25, lon=13.37)
```

`Point` also accepts `latitude` and `longitude` as keyword arguments, since Tile38 responses use the longer names:

```python
Point(latitude=52.25, longitude=13.37)
```

## PointLike

Annotate a `PointField()` attribute with `PointLike` instead of `Point` to also accept a plain `(lat, lon)` tuple. Pydantic converts either form into a real `Point`.

```python
from tileorm import Model, PointField, PointLike


class Truck(Model):
    location: PointLike = PointField()
```

```python
truck = Truck(location=(52.25, 13.37))
truck.location
# Point(lat=52.25, lon=13.37)
```

## Bounds

`Bounds` is a named tuple with `minlat`, `minlon`, `maxlat`, and `maxlon` fields. Use it as the value of a `BoundsField()`.

```python
from tileorm import Bounds, BoundsField, Model


class Zone(Model):
    area: Bounds = BoundsField()


zone = Zone(area=Bounds(minlat=52.0, minlon=13.0, maxlat=53.0, maxlon=14.0))
```

`Bounds` also accepts the long field names `minlatitude`, `minlongitude`, `maxlatitude`, and `maxlongitude` as keyword arguments.
