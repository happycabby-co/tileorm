import pytest

from pyle38 import Tile38

from tileorm.model import Model
from tileorm.types import Bounds, Point
from tileorm.fields import (
    BoundsField,
    Data,
    GeoHashField,
    Identifier,
    Group,
    CharField,
    JsonField,
    PointField,
)
from tileorm import exceptions


def test_must_have_identifier():
    class Truck(Model):
        location: tuple[float, float] = PointField()
        group: str = Group()
        field: str = CharField()

    with pytest.raises(exceptions.NoIdentifier):
        Truck(
            location=(0.0, 0.0),
            group="foo",
            field="bar",
        )


def test_multiple_identifiers_not_allowed():
    class Truck(Model):
        id: int = Identifier()
        id2: int = Identifier()
        group: str = Group()
        field: str = CharField()

    with pytest.raises(exceptions.MultipleIdentifiers):
        Truck(
            id=1,
            id2=2,
            location=(0.0, 0.0),
            group="foo",
            field="bar",
        )


@pytest.mark.asyncio
async def test_create(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: Point = PointField()
        field: str = CharField()

        class Meta:
            database = tile38

    await Truck.create(id=1, group="foo", location=(0.0, 0.0), field="bar")

    assert await tile38.exists("truck:group=foo", 1)

    truck = await tile38.get("truck:group=foo", 1).withfields().asObject()
    assert truck.object["coordinates"] == [0.0, 0.0]
    assert truck.fields["field"] == "bar"


@pytest.mark.asyncio
async def test_create_with_null_value(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: Point = PointField()
        field: str | None = CharField()

        class Meta:
            database = tile38

    await Truck.create(id=1, group="foo", location=(0.0, 0.0), field=None)

    assert await tile38.exists("truck:group=foo", 1)

    truck = await tile38.get("truck:group=foo", 1).withfields().asObject()
    assert truck.object["coordinates"] == [0.0, 0.0]
    assert truck.fields["field"] is None


@pytest.mark.asyncio
async def test_create_with_point(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: Point = PointField()

        class Meta:
            database = tile38

    await Truck.create(id=1, group="foo", location=Point(0.0, 0.0), field=None)

    assert await tile38.exists("truck:group=foo", 1)

    truck = await tile38.get("truck:group=foo", 1).withfields().asObject()
    assert truck.object["coordinates"] == [0.0, 0.0]


@pytest.mark.asyncio
async def test_create_with_hash(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: str = GeoHashField()

        class Meta:
            database = tile38

    await Truck.create(id=1, group="foo", location="gcpvn231e", field="bar")

    assert await tile38.exists("truck:group=foo", 1)

    truck = await tile38.get("truck:group=foo", 1).withfields().asObject()
    assert truck.object["coordinates"] == pytest.approx(
        [-0.075381, 51.505558], rel=1e-3
    )


@pytest.mark.asyncio
async def test_create_with_bounds(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: Bounds = BoundsField()

        class Meta:
            database = tile38

    await Truck.create(id=1, group="foo", location=(0.0, 0.0, 0.0, 0.0), field="bar")

    assert await tile38.exists("truck:group=foo", 1)

    truck = await tile38.get("truck:group=foo", 1).withfields().asObject()
    assert truck.object["coordinates"] == [[[0, 0], [0, 0], [0, 0], [0, 0], [0, 0]]]


@pytest.mark.asyncio
async def test_create_with_json(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: Point = PointField()
        field: list[dict[str, int]] = JsonField()

        class Meta:
            database = tile38

    await Truck.create(id=1, group="foo", location=Point(0.0, 0.0), field=[{"test": 1}])

    truck = await tile38.get("truck:group=foo", 1).withfields().asObject()
    assert truck.object["field"] == [{"test": 1}]


@pytest.mark.asyncio
async def test_get_with_point(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: Point = PointField()

        class Meta:
            database = tile38

    await Truck.create(id=1, group="foo", location=(0.0, 0.0))

    truck = await Truck.get(1, group="foo")
    assert isinstance(truck, Truck)
    assert truck.id == 1
    assert truck.group == "foo"
    assert truck.location == (0.0, 0.0)


@pytest.mark.asyncio
async def test_get_with_geohash(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: str = GeoHashField()

        class Meta:
            database = tile38

    await Truck.create(id=1, group="foo", location="gcpvn231e")

    truck = await Truck.get(1, group="foo")
    assert isinstance(truck, Truck)
    assert truck.id == 1
    assert truck.group == "foo"
    assert truck.location == "gcpvn231e"


@pytest.mark.asyncio
async def test_get_with_bounds(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: Bounds = BoundsField()

        class Meta:
            database = tile38

    await Truck.create(id=1, group="foo", location=(0.0, 1.0, 2.0, 3.0))

    truck = await Truck.get(1, group="foo")
    assert isinstance(truck, Truck)
    assert truck.id == 1
    assert truck.group == "foo"
    assert truck.location == (0.0, 1.0, 2.0, 3.0)


@pytest.mark.asyncio
async def test_get_with_json(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: Point = PointField()
        field: list[dict[str, int]] = JsonField()

        class Meta:
            database = tile38

    await Truck.create(id=1, group="foo", location=Point(0.0, 0.0), field=[{"test": 1}])

    truck = await Truck.get(1, group="foo")
    assert truck.field == [{"test": 1}]


@pytest.mark.asyncio
async def test_get_not_found(tile38: Tile38):
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: Point = PointField()
        field: str = CharField()

        class Meta:
            database = tile38

    with pytest.raises(exceptions.NotFoundError):
        await Truck.get(1, group="foo")
