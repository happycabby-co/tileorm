---
title: tileorm.types
description: Point, PointLike, and Bounds.
---

## Point

```python
class Point(NamedTuple):
    lat: float
    lon: float
```

Accepts `lat`/`lon` or `latitude`/`longitude` as keyword arguments.

## PointLike

```python
PointLike = Point | tuple[float, float]
```

Use as a type annotation on a `PointField()` attribute to also accept a plain `(lat, lon)` tuple.

## Bounds

```python
class Bounds(NamedTuple):
    minlat: float
    minlon: float
    maxlat: float
    maxlon: float
```

Accepts `minlat`/`minlon`/`maxlat`/`maxlon` or `minlatitude`/`minlongitude`/`maxlatitude`/`maxlongitude` as keyword arguments.

See [Geo types](/guides/geo-types/) for usage examples.
