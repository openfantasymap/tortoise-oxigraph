"""
OxigraphExecutor – translates Tortoise ORM CRUD operations to RDF / SPARQL.

Instead of generating and parsing SQL, this executor works directly with
the pyoxigraph Store via the OxigraphClient.  The base class still builds
SQL queries for SELECT (via pypika), but we intercept them at the
execute_select() level and translate to SPARQL.

INSERT / UPDATE / DELETE are handled entirely in RDF without any SQL.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Iterable
from typing import Any

import pyoxigraph as ox
from tortoise.backends.base.executor import BaseExecutor
from tortoise.exceptions import OperationalError
from tortoise.fields.relational import (
    BackwardFKRelation,
    BackwardOneToOneRelation,
    ForeignKeyFieldInstance,
    ManyToManyFieldInstance,
    OneToOneFieldInstance,
)
from tortoise.fields.base import Field

from tortoise_oxigraph.rdf_utils import (
    _RDF_TYPE,
    field_predicate_iri,
    model_class_iri,
    model_instance_iri,
    next_sequence,
    python_to_term,
    term_to_python,
)

log = logging.getLogger("tortoise.backends.oxigraph.executor")

# Fields that map to relational references (skip or handle as FK IRI)
_RELATIONAL = (
    BackwardFKRelation,
    BackwardOneToOneRelation,
    ForeignKeyFieldInstance,
    ManyToManyFieldInstance,
    OneToOneFieldInstance,
)


class OxigraphExecutor(BaseExecutor):
    """Executor that stores model data as RDF triples in a pyoxigraph Store."""

    EXPLAIN_PREFIX = "# EXPLAIN"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _meta(self):
        return self.model._meta

    def _app(self) -> str:
        try:
            return self._meta().app or "default"
        except AttributeError:
            return "default"

    def _model_name(self) -> str:
        return self.model.__name__   # self.model IS the class, not MetaInfo

    def _type_iri(self) -> ox.NamedNode:
        return model_class_iri(self._app(), self._model_name())

    def _subject_iri(self, pk: Any) -> ox.NamedNode:
        return model_instance_iri(self._app(), self._model_name(), pk)

    def _field_pred(self, col: str) -> ox.NamedNode:
        return field_predicate_iri(self._app(), self._model_name(), col)

    @property
    def _store(self) -> ox.Store:
        return self.db.store  # type: ignore[union-attr]

    async def _ensure_connected(self) -> None:
        """Ensure the underlying store is open before use."""
        await self.db.ensure_connected()  # type: ignore[union-attr]

    def _db_columns(self) -> list[str]:
        """All DB column names that should be persisted (skip relational)."""
        meta = self._meta()
        result = []
        for field_name, col in meta.fields_db_projection.items():
            field_obj = meta.fields_map.get(field_name)
            if field_obj and isinstance(field_obj, _RELATIONAL):
                continue
            result.append((field_name, col))
        return result

    def _register(self) -> None:
        """Register this model's db_table → meta in the client for SQL→SPARQL."""
        try:
            self.db.register_model_meta(self._meta().db_table, self._meta())
        except AttributeError:
            pass

    # ------------------------------------------------------------------
    # _process_insert_result  (required abstract method)
    # ------------------------------------------------------------------

    async def _process_insert_result(self, instance: Any, results: Any) -> None:
        """Set auto-generated PK on instance after INSERT."""
        pk_field = self._meta().pk
        if pk_field is None:
            return
        if getattr(pk_field, "generated", False):
            # results is the new pk value we stored
            instance.pk = results

    # ------------------------------------------------------------------
    # INSERT
    # ------------------------------------------------------------------

    async def execute_insert(self, instance: Any) -> None:
        await self._ensure_connected()
        self._register()
        meta = self._meta()
        pk_field = meta.pk

        # Determine / generate PK
        if pk_field is not None and getattr(pk_field, "generated", False) and not instance._custom_generated_pk:
            from tortoise.fields import (IntField, SmallIntField, BigIntField)
            if isinstance(pk_field, (IntField, SmallIntField, BigIntField)):
                pk_val = await self.db.run_in_executor(
                    next_sequence, self._store, self._app(), self._model_name()
                )
                setattr(instance, pk_field.model_field_name, pk_val)
            elif isinstance(pk_field.__class__, type) and issubclass(pk_field.__class__,
                    __import__("tortoise.fields.data", fromlist=["UUIDField"]).UUIDField.__class__):
                pk_val = uuid.uuid4()
                setattr(instance, pk_field.model_field_name, pk_val)

        pk_val = instance.pk
        if pk_val is None:
            raise OperationalError("Cannot insert a record without a primary key value")

        subj = self._subject_iri(pk_val)

        quads: list[ox.Quad] = [
            ox.Quad(subj, _RDF_TYPE, self._type_iri(), ox.DefaultGraph()),
        ]

        for field_name, col in self._db_columns():
            value = getattr(instance, field_name, None)
            if value is None:
                continue
            try:
                # to_db_value may apply serialisation (e.g. JSON, timezone)
                field_obj = meta.fields_map[field_name]
                db_val = field_obj.to_db_value(value, instance)
            except Exception:
                db_val = value

            if db_val is None:
                continue

            try:
                term = python_to_term(db_val)
            except Exception:
                term = ox.Literal(str(db_val))

            quads.append(ox.Quad(subj, self._field_pred(col), term, ox.DefaultGraph()))

        await self.db.run_in_executor(self._store.extend, quads)
        await self._process_insert_result(instance, pk_val)

    # ------------------------------------------------------------------
    # BULK INSERT
    # ------------------------------------------------------------------

    async def execute_bulk_insert(
        self, instances: Iterable[Any], batch_size: int | None = None
    ) -> None:
        await self._ensure_connected()
        self._register()
        for instance in instances:
            await self.execute_insert(instance)

    # ------------------------------------------------------------------
    # UPDATE
    # ------------------------------------------------------------------

    async def execute_update(
        self, instance: Any, update_fields: Iterable[str] | None
    ) -> int:
        await self._ensure_connected()
        self._register()
        meta = self._meta()
        pk_val = instance.pk
        if pk_val is None:
            raise OperationalError("Cannot update a record without a primary key")

        subj = self._subject_iri(pk_val)

        # Determine which fields to update
        if update_fields is not None:
            fields_to_update = list(update_fields)
        else:
            fields_to_update = [
                fn for fn, _ in self._db_columns()
                if not meta.fields_map[fn].pk
            ]

        for field_name in fields_to_update:
            field_obj = meta.fields_map.get(field_name)
            if field_obj is None or field_obj.pk:
                continue
            col = meta.fields_db_projection.get(field_name)
            if col is None:
                continue

            pred = self._field_pred(col)
            value = getattr(instance, field_name, None)

            # Remove existing triple for this field
            to_remove = list(self._store.quads_for_pattern(subj, pred, None, None))
            for quad in to_remove:
                self._store.remove(quad)

            if value is not None:
                try:
                    db_val = field_obj.to_db_value(value, instance)
                except Exception:
                    db_val = value

                if db_val is not None:
                    try:
                        term = python_to_term(db_val)
                    except Exception:
                        term = ox.Literal(str(db_val))
                    self._store.add(ox.Quad(subj, pred, term, ox.DefaultGraph()))

        return 1

    # ------------------------------------------------------------------
    # DELETE
    # ------------------------------------------------------------------

    async def execute_delete(self, instance: Any) -> int:
        await self._ensure_connected()
        self._register()
        pk_val = instance.pk
        if pk_val is None:
            return 0

        subj = self._subject_iri(pk_val)
        to_remove = list(self._store.quads_for_pattern(subj, None, None, None))
        for quad in to_remove:
            self._store.remove(quad)
        return len(to_remove)

    # ------------------------------------------------------------------
    # SELECT  (intercept SQL, translate to SPARQL)
    # ------------------------------------------------------------------

    async def execute_select(
        self,
        sql: str,
        values: list | None = None,
        custom_fields: list | None = None,
    ) -> list:
        await self._ensure_connected()
        self._register()
        from tortoise_oxigraph.filters import parse_select
        from tortoise_oxigraph.sparql_builder import build_sparql_select

        parsed = parse_select(sql, values)

        # Override table with actual meta table so resolution works
        parsed.table = self._meta().db_table

        sparql = build_sparql_select(
            parsed,
            {parsed.table: self._meta()},
        )
        log.debug("SPARQL SELECT:\n%s", sparql)

        if parsed.is_count:
            rows = await self.db.run_in_executor(self._run_count, sparql)
        else:
            rows = await self.db.run_in_executor(self._run_select, sparql, parsed)

        # Build model instances from rows
        return await self._build_instances(rows, custom_fields)

    def _run_count(self, sparql: str) -> list[dict]:
        results = self._store.query(sparql)
        for solution in results:
            try:
                count_val = solution["count"]
                return [{"count": int(count_val.value)}]
            except Exception:
                # fallback: try first variable
                try:
                    count_val = solution[0]
                    return [{"count": int(count_val.value)}]
                except Exception:
                    pass
        return [{"count": 0}]

    def _run_select(self, sparql: str, parsed) -> list[dict]:
        results = self._store.query(sparql)
        # Variable names are on the QuerySolutions object, not per-solution
        var_names = [v.value for v in results.variables]

        rows: list[dict] = []
        meta = self._meta()
        col_to_field = {v: k for k, v in meta.fields_db_projection.items()}

        for solution in results:
            row: dict[str, Any] = {}
            for var_name in var_names:
                if var_name == "_subject":
                    continue
                try:
                    term = solution[var_name]
                except Exception:
                    term = None
                if term is None:
                    row[var_name] = None
                    continue
                raw = term_to_python(term)
                # Convert back through the field's from_db_value if available
                field_name = col_to_field.get(var_name)
                if field_name:
                    field_obj = meta.fields_map.get(field_name)
                    if field_obj is not None:
                        try:
                            raw = field_obj.from_db_value(raw, None)
                        except Exception:
                            pass
                row[var_name] = raw
            rows.append(row)
        return rows

    async def _build_instances(
        self, rows: list[dict], custom_fields: list | None
    ) -> list:
        meta = self._meta()
        instance_list = []

        for row in rows:
            # Re-key: DB column names → model field names expected by _init_from_db
            db_row: dict[str, Any] = {}
            for col, val in row.items():
                if col == "count":
                    db_row[col] = val
                else:
                    db_row[col] = val
            instance = self.model._init_from_db(**db_row)
            if custom_fields:
                for field in custom_fields:
                    object.__setattr__(instance, field, row.get(field))
            instance_list.append(instance)

        await self._execute_prefetch_queries(instance_list)
        return instance_list

    # ------------------------------------------------------------------
    # EXPLAIN  (SPARQL doesn't have EXPLAIN; return empty)
    # ------------------------------------------------------------------

    async def execute_explain(self, sql: str) -> Any:
        return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _solution_vars(solution) -> list[str]:
    """Return variable names from a QuerySolution."""
    try:
        return [str(v) for v in solution.variables]
    except AttributeError:
        pass
    try:
        return list(solution.keys())
    except AttributeError:
        pass
    # Fallback: iterate
    try:
        return list(vars(solution).keys())
    except Exception:
        return []
