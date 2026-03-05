"""
Basic end-to-end tests for the tortoise-oxigraph backend.

Tests cover:
  * init / teardown
  * create / save (INSERT)
  * fetch by pk (SELECT with WHERE pk=?)
  * filter (SELECT with WHERE col=?)
  * update (UPDATE)
  * delete (DELETE)
  * count (SELECT COUNT)
  * bulk create
  * ordering + limit
  * NULL field handling
"""

from __future__ import annotations

import pyoxigraph as ox
import pytest
from tortoise import Tortoise, fields
from tortoise.connection import connections
from tortoise.models import Model

# Ensure the oxigraph backend is registered
import tortoise_oxigraph  # noqa: F401


# ---------------------------------------------------------------------------
# Models under test
# ---------------------------------------------------------------------------

class Tournament(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)
    desc = fields.TextField(null=True)

    class Meta:
        table = "tournament"
        app = "models"


class Event(Model):
    id = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)
    prize = fields.DecimalField(max_digits=10, decimal_places=2, null=True)

    class Meta:
        table = "event"
        app = "models"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
async def init_db():
    await Tortoise.init(
        db_url="oxigraph://:memory:",
        modules={"models": [__name__]},
    )
    await Tortoise.generate_schemas()
    yield
    await Tortoise.close_connections()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_create_and_fetch_by_pk():
    t = await Tournament.create(name="Wimbledon")
    assert t.id is not None
    assert t.id >= 1

    fetched = await Tournament.get(id=t.id)
    assert fetched.name == "Wimbledon"


@pytest.mark.asyncio
async def test_create_multiple_auto_pk():
    t1 = await Tournament.create(name="US Open")
    t2 = await Tournament.create(name="Roland Garros")
    assert t1.id != t2.id
    assert t2.id == t1.id + 1


@pytest.mark.asyncio
async def test_filter_by_name():
    await Tournament.create(name="Wimbledon")
    await Tournament.create(name="US Open")

    results = await Tournament.filter(name="Wimbledon")
    assert len(results) == 1
    assert results[0].name == "Wimbledon"


@pytest.mark.asyncio
async def test_all():
    await Tournament.create(name="A")
    await Tournament.create(name="B")
    await Tournament.create(name="C")

    all_t = await Tournament.all()
    assert len(all_t) == 3


@pytest.mark.asyncio
async def test_update():
    t = await Tournament.create(name="Old Name")
    t.name = "New Name"
    await t.save()

    refreshed = await Tournament.get(id=t.id)
    assert refreshed.name == "New Name"


@pytest.mark.asyncio
async def test_delete():
    t = await Tournament.create(name="To Delete")
    tid = t.id
    await t.delete()

    count = await Tournament.filter(id=tid).count()
    assert count == 0


@pytest.mark.asyncio
async def test_count():
    for i in range(5):
        await Tournament.create(name=f"T{i}")
    n = await Tournament.all().count()
    assert n == 5


@pytest.mark.asyncio
async def test_null_field():
    t = await Tournament.create(name="No Desc", desc=None)
    fetched = await Tournament.get(id=t.id)
    assert fetched.desc is None


@pytest.mark.asyncio
async def test_null_field_set():
    t = await Tournament.create(name="With Desc", desc="Hello")
    fetched = await Tournament.get(id=t.id)
    assert fetched.desc == "Hello"


@pytest.mark.asyncio
async def test_bulk_create():
    objs = [Tournament(name=f"B{i}") for i in range(4)]
    await Tournament.bulk_create(objs)
    n = await Tournament.all().count()
    assert n == 4


@pytest.mark.asyncio
async def test_order_by_name():
    await Tournament.create(name="Zorro")
    await Tournament.create(name="Alpha")
    await Tournament.create(name="Middle")

    ordered = await Tournament.all().order_by("name")
    names = [t.name for t in ordered]
    assert names == sorted(names)


@pytest.mark.asyncio
async def test_limit():
    for i in range(10):
        await Tournament.create(name=f"T{i}")
    subset = await Tournament.all().limit(3)
    assert len(subset) == 3


@pytest.mark.asyncio
async def test_decimal_field():
    from decimal import Decimal
    e = await Event.create(name="Grand Prix", prize=Decimal("50000.00"))
    fetched = await Event.get(id=e.id)
    assert fetched.prize == Decimal("50000.00")


@pytest.mark.asyncio
async def test_get_or_none_missing():
    result = await Tournament.get_or_none(id=9999)
    assert result is None


@pytest.mark.asyncio
async def test_graph_scoped_reads_and_writes():
    conn = connections.get("default")

    async with conn.graph_scope(write_graph="urn:graph:g1", read_graphs=["urn:graph:g1"]):
        await Tournament.create(name="Graph One")

    async with conn.graph_scope(write_graph="urn:graph:g2", read_graphs=["urn:graph:g2"]):
        await Tournament.create(name="Graph Two")

    assert await Tournament.all().count() == 0

    async with conn.graph_scope(read_graphs=["urn:graph:g1"]):
        graph_one = await Tournament.all()
        assert [t.name for t in graph_one] == ["Graph One"]

    async with conn.graph_scope(read_graphs=["urn:graph:g1", "urn:graph:g2"]):
        merged = await Tournament.all().order_by("name")
        assert [t.name for t in merged] == ["Graph One", "Graph Two"]


@pytest.mark.asyncio
async def test_initialization_mode_bootstrap_required_ontologies():
    await Tortoise.close_connections()
    await Tortoise.init(
        config={
            "connections": {
                "default": {
                    "engine": "tortoise_oxigraph",
                    "credentials": {
                        "store_path": ":memory:",
                        "initialization_mode": "bootstrap",
                        "required_ontology_iris": ["urn:ontology:core"],
                        "ontology_graph": "urn:graph:ontology",
                    },
                }
            },
            "apps": {"models": {"models": [__name__], "default_connection": "default"}},
        }
    )
    await Tortoise.generate_schemas()

    conn = connections.get("default")
    await conn.ensure_connected()
    check = await conn.run_in_executor(
        lambda: bool(
            list(
                conn.store.quads_for_pattern(
                    None,
                    None,
                    None,
                    ox.NamedNode("urn:graph:ontology"),
                )
            )
        )
    )
    assert check is True
