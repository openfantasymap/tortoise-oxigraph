# tortoise-oxigraph

A [Tortoise ORM](https://tortoise.github.io/) database backend backed by [pyoxigraph](https://pyoxigraph.readthedocs.io/) (RDF / SPARQL).

## Overview

This contrib package lets you use Tortoise ORM models with a [pyoxigraph](https://pyoxigraph.readthedocs.io/) RDF store as the storage backend — no SQL database required.
Each model instance is stored as a set of RDF triples. Queries are translated from Tortoise's pypika-generated SQL into SPARQL at runtime.

## Data model

```
<tortoise://app/Model/pk>  rdf:type                      <tortoise://app/Model>
<tortoise://app/Model/pk>  <tortoise://app/Model#name>   "Alice"^^xsd:string
<tortoise://app/Model/pk>  <tortoise://app/Model#age>    "30"^^xsd:integer
```

## Installation

```bash
pip install tortoise-oxigraph
```

## Usage

```python
import tortoise_oxigraph  # registers the "oxigraph" URL scheme

from tortoise import Tortoise
from tortoise.models import Model
from tortoise import fields

class Tournament(Model):
    id   = fields.IntField(primary_key=True)
    name = fields.CharField(max_length=100)

async def main():
    await Tortoise.init(
        db_url="oxigraph://:memory:",          # ephemeral in-memory store
        # db_url="oxigraph:///path/to/store",  # persistent RocksDB store
        modules={"models": [__name__]},
    )
    await Tortoise.generate_schemas()          # no-op, but good practice

    t = await Tournament.create(name="Wimbledon")
    print(t.id, t.name)

    results = await Tournament.filter(name="Wimbledon")
    t.name = "Wimbledon 2026"
    await t.save()
    await t.delete()

    await Tortoise.close_connections()
```


## Ontology initialization and named-graph routing

The backend now supports optional ontology enforcement and graph-scoped execution:

- `initialization_mode="none" | "enforce" | "bootstrap"`
- `required_ontology_iris=["urn:ontology:core", ...]`
- `ontology_graph="urn:graph:ontology"` (optional, defaults to the default graph)

`bootstrap` writes minimal `owl:Ontology` declarations for required ontologies at connection time, then validates them.
`enforce` only validates that those ontology IRIs already exist in the store.

You can also route operations to named graphs and merge read graphs dynamically:

```python
from tortoise.connection import connections

conn = connections.get("default")

# write + read graph scoping
async with conn.graph_scope(write_graph="urn:graph:g1", read_graphs=["urn:graph:g1"]):
    await Tournament.create(name="Scoped")

# dynamic merge (server-side query union across named graphs)
async with conn.graph_scope(read_graphs=["urn:graph:g1", "urn:graph:g2"]):
    results = await Tournament.all()
```

## URL format

| URL | Store type |
|-----|------------|
| `oxigraph://:memory:` | Ephemeral in-memory store (no persistence) |
| `oxigraph:///path/to/store` | Persistent on-disk RocksDB store |

## Supported operations

- `Model.create()` / `instance.save(force_create=True)`
- `Model.get()`, `Model.get_or_none()`, `Model.filter()`, `Model.all()`
- `instance.save()` (update)
- `instance.delete()`
- `.count()`, `.order_by()`, `.limit()`, `.offset()`
- `Model.bulk_create()`
- Field types: `IntField`, `SmallIntField`, `BigIntField`, `CharField`, `TextField`, `FloatField`, `DecimalField`, `BooleanField`, `DatetimeField`, `DateField`, `TimeField`, `UUIDField`, `BinaryField`
- Null field handling
- WHERE filters: `=`, `!=`, `<`, `>`, `<=`, `>=`, `IS NULL`, `IS NOT NULL`, `IN`, `NOT IN`, `LIKE`, `BETWEEN`

## Architecture

```
tortoise_oxigraph/
├── __init__.py          # registers "oxigraph" URL scheme with Tortoise
├── client.py            # OxigraphClient (BaseDBAsyncClient)
├── executor.py          # OxigraphExecutor (BaseExecutor) — SPARQL CRUD
├── schema_generator.py  # OxigraphSchemaGenerator (no-op; RDF is schemaless)
├── filters.py           # SQL WHERE clause → ParsedSelect
├── sparql_builder.py    # ParsedSelect → SPARQL SELECT
└── rdf_utils.py         # IRI helpers, Python ↔ RDF type mapping, sequences
```

## License

Apache-2.0
