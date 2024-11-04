# 🌐 TileORM

[![Build](https://github.com/alxwrd/tileorm/actions/workflows/build.yaml/badge.svg)](https://github.com/alxwrd/tileorm/actions/workflows/deploy.yaml)

> [!WARNING]
> Not advisable for production critical workflows


## Getting started

```shell
pip install tileorm
```

```python
from tileorm import Model, Identifier, Group, CharField, Tile38

db = Tile38("redis://localhost:9851")

class Truck(Model):
    id: int = Identifier()
    group: str = Group()
    field: str = CharField()

    class Meta:
        database = db


truck1 = await Truck.create(
    id=1,
    group="fleet1",
    location=Point(lat=52.25, lon=13.37),
    field="value,
)

truck = Truck.get(id=1, group="fleet1") 
# Truck(id=1, location=Location(lat=52.25, lon=13.37), group='fleet1', field='value')

await db.get(truck.key, truck.id).withfields().exec()
# {'ok': True, 'object': {'type': 'Point', 'coordinates': [13.37, 52.25]}, 'fields': {'field': 'value'}, 'elapsed': '411.458µs'}
```
