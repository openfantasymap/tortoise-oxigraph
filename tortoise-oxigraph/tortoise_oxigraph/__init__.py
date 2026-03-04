"""
tortoise-oxigraph
=================
A Tortoise ORM database backend backed by pyoxigraph (RDF / SPARQL).

Usage
-----
Register the backend and use it in your Tortoise config::

    await Tortoise.init(
        db_url="oxigraph://:memory:",
        modules={"models": ["myapp.models"]},
    )

    # or on-disk:
    await Tortoise.init(
        db_url="oxigraph:///path/to/store",
        modules={"models": ["myapp.models"]},
    )

URL format
----------
    oxigraph://:memory:          →  ephemeral in-memory store
    oxigraph:///path/to/store    →  persistent RocksDB store

Registration
------------
Importing this package monkey-patches
``tortoise.backends.base.config_generator.DB_LOOKUP`` to add the
``"oxigraph"`` scheme entry, so Tortoise's ``expand_db_url`` can resolve it.
"""

from __future__ import annotations

from tortoise_oxigraph.client import OxigraphClient
from tortoise_oxigraph.executor import OxigraphExecutor
from tortoise_oxigraph.schema_generator import OxigraphSchemaGenerator

__all__ = ["OxigraphClient", "OxigraphExecutor", "OxigraphSchemaGenerator"]
__version__ = "0.1.0"

# ---------------------------------------------------------------------------
# Register the "oxigraph" URL scheme with Tortoise
# ---------------------------------------------------------------------------

def _register() -> None:
    try:
        from tortoise.backends.base.config_generator import DB_LOOKUP
    except ImportError:
        return  # tortoise not installed – skip

    if "oxigraph" not in DB_LOOKUP:
        DB_LOOKUP["oxigraph"] = {
            "engine": "tortoise_oxigraph",
            # skip_first_char=False keeps the full path including the leading /
            "skip_first_char": False,
            "vmap": {"path": "store_path"},
            "defaults": {},
            "cast": {},
        }


_register()

# Module-level client_class so Tortoise can discover the backend
client_class = OxigraphClient


def get_client_class(db_info: dict) -> type[OxigraphClient]:
    """Factory called by Tortoise's connection bootstrapper."""
    return OxigraphClient
