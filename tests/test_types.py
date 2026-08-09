"""Tests for static types and runtime types.

These tests check the field factories. For example, they check
that `x: float = FloatField(...)` does not cause a type error.

These tests also check that the methods `Model.get`, `get_by_key`,
`find`, `find_all`, and `nearby` have the return type `Self`.

The tool `ty` checks the types in this file. Run `poe check` to
start `ty`. `ty` checks the whole project, not only this file.

The function `assert_type` does nothing at runtime. Pytest always
passes each `assert_type` call. But if a field factory or a
`Self`-typed model method has a type error, `ty` will report a
failure for this file.
"""

from typing import assert_type

import pytest
import pytest_asyncio
from pydantic import BaseModel
from pyle38 import Tile38

from tileorm import (
    CharField,
    FloatField,
    Identifier,
    IntegerField,
    JsonField,
    Model,
    PointField,
)
from tileorm.types import Point, PointLike


class Step(BaseModel):
    name: str


class Job(Model):
    id: str = Identifier()
    location: Point = PointField()

    attempts: int = IntegerField(default=0)
    name: str = CharField(default="job")
    will_wait: float = FloatField(default=1200)
    route: list[Step] = JsonField(default_factory=list)


def test_point_like_field_accepts_plain_tuple():
    """PointLike widens accepted input, but Pydantic still coerces to a real Point."""

    class Delivery(Model):
        id: str = Identifier()
        location: PointLike = PointField()

    delivery = Delivery(id="1", location=(0.0, 0.0))

    assert_type(delivery.location, PointLike)
    assert isinstance(delivery.location, Point)
    assert delivery.location.lat == 0.0
    assert delivery.location.lon == 0.0


def test_field_factories_produce_correctly_typed_attributes():
    job = Job(id="1", location=Point(0.0, 0.0))

    assert_type(job.will_wait, float)
    assert_type(job.attempts, int)
    assert_type(job.name, str)
    assert_type(job.route, list[Step])

    assert job.will_wait == 1200
    assert job.attempts == 0
    assert job.name == "job"
    assert job.route == []


@pytest.mark.asyncio
async def test_nearby_accepts_plain_tuple(job):
    """Pydantic coerces a plain tuple into a Point, so `nearby` should accept one too."""
    async for result in Job.nearby((0.0, 0.0)):
        assert_type(result, Job)
        assert result.id == "1"


def test_field_factories_accept_explicit_values():
    job = Job(
        id="1",
        location=Point(0.0, 0.0),
        will_wait=30.0,
        attempts=2,
        name="custom",
        route=[Step(name="a"), Step(name="b")],
    )

    assert job.will_wait == 30.0
    assert job.attempts == 2
    assert job.route == [Step(name="a"), Step(name="b")]


@pytest_asyncio.fixture
async def job(tile38: Tile38):
    """Job backed by the real (dockerized) Tile38 instance used by test_model.py."""
    Job.Meta.database = tile38
    await Job.create(
        id="1", location=Point(0.0, 0.0), will_wait=30.0, attempts=1, name="a"
    )
    try:
        yield
    finally:
        Job.Meta.database = None


@pytest.mark.asyncio
async def test_get_returns_self_type(job):
    result = await Job.get("1")

    assert_type(result, Job)
    assert result.id == "1"
    assert result.will_wait == 30.0


@pytest.mark.asyncio
async def test_get_by_key_returns_self_type(job):
    result = await Job.get_by_key("1", Job._make_key())

    assert_type(result, Job)
    assert result.id == "1"


@pytest.mark.asyncio
async def test_find_yields_self_type(job):
    async for result in Job.find():
        assert_type(result, Job)
        assert result.id == "1"


@pytest.mark.asyncio
async def test_find_all_returns_list_of_self_type(job):
    results = await Job.find_all()

    assert_type(results, list[Job])
    assert [result.id for result in results] == ["1"]


@pytest.mark.asyncio
async def test_nearby_yields_self_type(job):
    async for result in Job.nearby(Point(0.0, 0.0)):
        assert_type(result, Job)
        assert result.id == "1"
