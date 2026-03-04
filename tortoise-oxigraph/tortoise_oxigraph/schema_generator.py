"""
OxigraphSchemaGenerator – no-op DDL generator.

pyoxigraph is a schema-less RDF store.  Tortoise's generate_schema()
call is intercepted here and turned into a no-op.  The RDF "schema" is
implicit in the IRI naming convention and rdf:type triples.
"""

from __future__ import annotations

import logging
from typing import Any

from tortoise.backends.base.schema_generator import BaseSchemaGenerator

log = logging.getLogger("tortoise.backends.oxigraph.schema")


class OxigraphSchemaGenerator(BaseSchemaGenerator):
    """No-op schema generator for the oxigraph backend."""

    DIALECT = "oxigraph"

    # ------------------------------------------------------------------
    # Required abstract method implementations
    # ------------------------------------------------------------------

    def _table_comment_generator(self, table: str, comment: str) -> str:
        return ""

    def _column_comment_generator(self, table: str, column: str, comment: str) -> str:
        return ""

    def _column_default_generator(
        self, table: str, column: str, default: Any
    ) -> str:
        return ""

    def _escape_default_value(self, default: Any):  # type: ignore[override]
        return default

    # ------------------------------------------------------------------
    # Override the actual schema-generation entry point
    # ------------------------------------------------------------------

    async def generate_from_string(self, creation_string: str) -> None:
        log.debug("generate_from_string (no-op for oxigraph)")

    def get_create_schema_sql(self, safe: bool = True) -> str:
        return ""

    def quote(self, val: str) -> str:
        return f'"{val}"'
