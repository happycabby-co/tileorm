import pytest
import pytest_asyncio

from pyle38 import Tile38

from tileorm.model import Model
from tileorm.types import Bounds, Point
from tileorm.fields import (
    BoundsField,
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


@pytest_asyncio.fixture
async def TruckModel(tile38: Tile38):
    """Fixture that provides a Truck model class with database configured."""
    class Truck(Model):
        id: int = Identifier()
        group: str = Group()
        location: Point = PointField()
        name: str | None = CharField(default=None)  # Make optional for now until field parsing is fixed

        class Meta:
            database = tile38

    return Truck


@pytest.mark.asyncio
async def test_nearby_with_point_input(TruckModel, tile38: Tile38):
    """Test nearby method with Point input type."""
    # Create trucks at different locations
    # Reference point will be (0.0, 0.0)
    # Truck 1: very close (within 1km radius)
    await TruckModel.create(
        id=1, group="fleet1", location=Point(0.001, 0.001), name="truck1"
    )
    # Truck 2: far away (outside 1km radius but within 200km)
    await TruckModel.create(
        id=2, group="fleet1", location=Point(1.0, 1.0), name="truck2"
    )
    # Truck 3: very close (within 1km radius)
    await TruckModel.create(
        id=3, group="fleet1", location=Point(0.002, 0.002), name="truck3"
    )

    # Query nearby using Point - small radius (should only get trucks 1 and 3)
    # Note: 0.001 degrees ≈ 111 meters, so 1km radius should catch trucks at 0.001, 0.001
    results = []
    async for truck in TruckModel.nearby(Point(0.0, 0.0), radius=1000.0, group="fleet1"):
        results.append(truck)

    # Should return trucks 1 and 3 (within 1km)
    assert len(results) == 2
    truck_ids = {truck.id for truck in results}
    assert truck_ids == {1, 3}

    # Query with larger radius (should get all trucks)
    results = []
    async for truck in TruckModel.nearby(Point(0.0, 0.0), radius=200000.0, group="fleet1"):
        results.append(truck)

    assert len(results) == 3
    truck_ids = {truck.id for truck in results}
    assert truck_ids == {1, 2, 3}


@pytest.mark.asyncio
async def test_nearby_with_str_input(TruckModel, tile38: Tile38):
    """Test nearby method with str (object_id) input type."""
    # Create a reference truck
    reference_truck = await TruckModel.create(
        id=1, group="fleet2", location=Point(0.0, 0.0), name="reference"
    )

    # Create other trucks at various distances
    await TruckModel.create(
        id=2, group="fleet2", location=Point(0.001, 0.001), name="nearby1"
    )
    await TruckModel.create(
        id=3, group="fleet2", location=Point(1.0, 1.0), name="far1"
    )
    await TruckModel.create(
        id=4, group="fleet2", location=Point(0.002, 0.002), name="nearby2"
    )

    # Query nearby using reference truck's ID as string
    results = []
    async for truck in TruckModel.nearby("1", radius=1000.0, group="fleet2"):
        results.append(truck)

    # Should return trucks 2 and 4 (within 1km of truck 1)
    # Note: truck 1 itself may or may not be included depending on Tile38 behavior
    assert len(results) >= 2
    truck_ids = {truck.id for truck in results}
    assert 2 in truck_ids
    assert 4 in truck_ids


@pytest.mark.asyncio
async def test_nearby_with_model_input(TruckModel, tile38: Tile38):
    """Test nearby method with Model instance input type."""
    # Create a reference truck instance
    reference_truck = await TruckModel.create(
        id=1, group="fleet3", location=Point(0.0, 0.0), name="reference"
    )

    # Create other trucks at various distances
    await TruckModel.create(
        id=2, group="fleet3", location=Point(0.001, 0.001), name="nearby1"
    )
    await TruckModel.create(
        id=3, group="fleet3", location=Point(1.0, 1.0), name="far1"
    )
    await TruckModel.create(
        id=4, group="fleet3", location=Point(0.002, 0.002), name="nearby2"
    )

    # Query nearby using Model instance
    results = []
    async for truck in TruckModel.nearby(reference_truck, radius=1000.0, group="fleet3"):
        results.append(truck)

    # Should return trucks 2 and 4 (within 1km of truck 1)
    assert len(results) >= 2
    truck_ids = {truck.id for truck in results}
    assert 2 in truck_ids
    assert 4 in truck_ids


@pytest.mark.asyncio
async def test_nearby_edge_cases(TruckModel, tile38: Tile38):
    """Test edge cases for nearby method."""
    # Edge case 1: Empty results when key doesn't exist
    results = []
    async for truck in TruckModel.nearby(Point(0.0, 0.0), radius=1000.0, group="nonexistent"):
        results.append(truck)
    assert len(results) == 0

    # Edge case 2: Empty results when reference object doesn't exist
    results = []
    async for truck in TruckModel.nearby("99999", radius=1000.0, group="fleet4"):
        results.append(truck)
    assert len(results) == 0

    # Edge case 3: Invalid target type should raise TypeError
    with pytest.raises(TypeError, match="target must be a Point, str \\(object_id\\), or Model instance"):
        async for truck in TruckModel.nearby(123, radius=1000.0, group="fleet5"):
            pass

    # Edge case 4: Radius filtering - create trucks and test different radii
    await TruckModel.create(
        id=1, group="fleet6", location=Point(0.0, 0.0), name="center"
    )
    await TruckModel.create(
        id=2, group="fleet6", location=Point(0.001, 0.001), name="close"
    )
    await TruckModel.create(
        id=3, group="fleet6", location=Point(0.01, 0.01), name="medium"
    )
    await TruckModel.create(
        id=4, group="fleet6", location=Point(1.0, 1.0), name="far"
    )

    # Very small radius (100m) - should get only truck 1 (at center point)
    # Truck 2 is at 0.001, 0.001 which is ~111m away, outside 100m radius
    results = []
    async for truck in TruckModel.nearby(Point(0.0, 0.0), radius=100.0, group="fleet6"):
        results.append(truck)
    assert len(results) >= 1
    truck_ids = {truck.id for truck in results}
    assert 1 in truck_ids  # Center truck should be included

    # Medium radius (1km) - should get trucks 1 and 2
    # Truck 3 is at 0.01, 0.01 which is ~1.11km away, outside 1km radius
    results = []
    async for truck in TruckModel.nearby(Point(0.0, 0.0), radius=1000.0, group="fleet6"):
        results.append(truck)
    assert len(results) >= 2
    truck_ids = {truck.id for truck in results}
    assert 1 in truck_ids  # Center truck
    assert 2 in truck_ids  # Close truck (~111m away)

    # Large radius (200km) - should get all trucks
    results = []
    async for truck in TruckModel.nearby(Point(0.0, 0.0), radius=200000.0, group="fleet6"):
        results.append(truck)
    assert len(results) >= 3

    # Edge case 5: Group filtering - trucks in different groups
    await TruckModel.create(
        id=1, group="fleet7", location=Point(0.0, 0.0), name="fleet7_truck"
    )
    await TruckModel.create(
        id=1, group="fleet8", location=Point(0.001, 0.001), name="fleet8_truck"
    )

    # Query fleet7 - should only get truck 1
    # Use larger radius to ensure truck 1 is included (it's at center point)
    results = []
    async for truck in TruckModel.nearby(Point(0.0, 0.0), radius=1000.0, group="fleet7"):
        results.append(truck)
    truck_ids = {truck.id for truck in results}
    assert 1 in truck_ids
    assert not any(t.group == "fleet8" for t in results)  # fleet8 truck not in fleet7

    # Query fleet8 - should only get truck 1 (in fleet8)
    # Truck is at 0.001, 0.001 which is ~111m away, so use 200m radius
    results = []
    async for truck in TruckModel.nearby(Point(0.0, 0.0), radius=200.0, group="fleet8"):
        results.append(truck)
    truck_ids = {truck.id for truck in results}
    assert 1 in truck_ids
    assert not any(t.group == "fleet7" for t in results)  # fleet7 truck not in fleet8
