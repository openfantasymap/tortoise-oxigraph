"""
OxigraphClient – Tortoise ORM database client backed by pyoxigraph.

URL format
----------
    oxigraph://:memory:          →  in-memory Store (no path)
    oxigraph:///path/to/store    →  on-disk RocksDB Store
    oxigraph://localhost/path    →  same (host ignored, path used)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from contextlib import asynccontextmanager
from typing import Any

import pyoxigraph as ox
from tortoise.backends.base.client import (
    BaseDBAsyncClient,
    Capabilities,
    ConnectionWrapper,
    TransactionContext,
)
from tortoise.exceptions import OperationalError

log = logging.getLogger("tortoise.backends.oxigraph")


# ---------------------------------------------------------------------------
# Internal connection wrapper
# ---------------------------------------------------------------------------

class _OxigraphConnectionWrapper:
    """
    Thin wrapper around pyoxigraph.Store that makes it look like an async
    connection.  pyoxigraph is synchronous; we offload blocking calls to a
    thread-pool executor via asyncio.
    """

    def __init__(self, store: ox.Store, loop: asyncio.AbstractEventLoop) -> None:
        self._store = store
        self._loop = loop

    @property
    def store(self) -> ox.Store:
        return self._store

    async def run(self, fn, *args, **kwargs):
        """Run a synchronous callable in the default thread-pool executor."""
        return await self._loop.run_in_executor(None, lambda: fn(*args, **kwargs))


class _OxigraphConnectionContextManager:
    """Async context manager returned by OxigraphClient.acquire_connection().

    Mirrors ConnectionWrapper.ensure_connection() so that the store is
    lazily opened the first time it is needed, just like aiosqlite backends.
    """

    def __init__(self, client: "OxigraphClient") -> None:
        self._client = client

    async def __aenter__(self) -> "_OxigraphConnectionWrapper":
        if self._client._connection is None:
            await self._client.create_connection(with_db=True)
        return self._client._connection  # type: ignore[return-value]

    async def __aexit__(self, *args: Any) -> None:
        pass


# ---------------------------------------------------------------------------
# Transaction wrapper
# ---------------------------------------------------------------------------

class OxigraphTransactionWrapper(BaseDBAsyncClient):
    """
    Wraps a client and records all SPARQL UPDATE statements issued during a
    transaction.  On commit they are replayed; on rollback the store is
    restored from a snapshot.

    pyoxigraph.Store guarantees atomic writes on commit, but has no native
    SQL-style BEGIN/ROLLBACK.  We implement optimistic rollback by keeping an
    in-process copy of the pre-transaction quads.
    """

    executor_class: type  # set by OxigraphClient after import
    schema_generator: type
    capabilities: Capabilities

    def __init__(self, client: "OxigraphClient") -> None:
        super().__init__(connection_name=client.connection_name, fetch_inserted=client.fetch_inserted)
        self._client = client
        self._snapshot: list[ox.Quad] = []
        self._finalized = False

    # ---- delegate everything to the underlying client ---------------------

    @property
    def _connection(self):  # type: ignore[override]
        return self._client._connection

    def acquire_connection(self) -> _OxigraphConnectionContextManager:
        return _OxigraphConnectionContextManager(self._client)

    async def execute_insert(self, query: str, values: list) -> Any:
        return await self._client.execute_insert(query, values)

    async def execute_query(self, query: str, values: list | None = None) -> tuple[int, Sequence[dict]]:
        return await self._client.execute_query(query, values)

    async def execute_query_dict(self, query: str, values: list | None = None) -> list[dict]:
        _, rows = await self.execute_query(query, values)
        return list(rows)

    async def execute_script(self, query: str) -> None:
        await self._client.execute_script(query)

    async def execute_many(self, query: str, values: list[list]) -> None:
        await self._client.execute_many(query, values)

    # ---- transaction lifecycle --------------------------------------------

    async def begin(self) -> None:
        store = self._client._connection.store  # type: ignore[union-attr]
        self._snapshot = list(store)   # cheap for small graphs; iterate quads

    async def commit(self) -> None:
        self._finalized = True
        self._snapshot = []

    async def rollback(self) -> None:
        if self._finalized:
            return
        store = self._client._connection.store  # type: ignore[union-attr]
        store.clear()
        store.extend(self._snapshot)
        self._finalized = True
        self._snapshot = []

    # ---- unused BaseDBAsyncClient abstract methods ------------------------

    async def create_connection(self, with_db: bool) -> None:  # pragma: no cover
        pass

    async def close(self) -> None:  # pragma: no cover
        pass

    async def db_create(self) -> None:  # pragma: no cover
        pass

    async def db_delete(self) -> None:  # pragma: no cover
        pass

    def _in_transaction(self) -> TransactionContext:  # pragma: no cover
        raise OperationalError("Nested transactions are not supported")


class _OxigraphTransactionContext:
    """Async context manager for a transaction block."""

    def __init__(self, wrapper: OxigraphTransactionWrapper) -> None:
        self._wrapper = wrapper
        self._token: Any = None

    async def __aenter__(self) -> OxigraphTransactionWrapper:
        from tortoise.context import set_current_connection
        await self._wrapper.begin()
        self._token = set_current_connection(
            self._wrapper._client.connection_name, self._wrapper
        )
        return self._wrapper

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if exc_type:
            await self._wrapper.rollback()
        else:
            await self._wrapper.commit()
        if self._token is not None:
            try:
                from tortoise.context import reset_current_connection
                reset_current_connection(self._wrapper._client.connection_name, self._token)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Main client
# ---------------------------------------------------------------------------

class OxigraphClient(BaseDBAsyncClient):
    """
    Tortoise ORM async client backed by a pyoxigraph Store.

    Credentials
    -----------
    ``store_path``:  filesystem path for the on-disk store, or
                     the string ``":memory:"`` for an ephemeral in-memory store.
    """

    from tortoise_oxigraph.executor import OxigraphExecutor as executor_class  # type: ignore[assignment]
    from tortoise_oxigraph.schema_generator import OxigraphSchemaGenerator as schema_generator  # type: ignore[assignment]

    capabilities = Capabilities(
        "oxigraph",
        daemon=False,
        requires_limit=False,
        inline_comment=True,
        supports_transactions=True,
        support_for_update=False,
        support_for_no_key_update=False,
        support_index_hint=False,
        support_update_limit_order_by=False,
        support_for_posix_regex_queries=False,
        support_json_attributes=False,
        can_rollback_ddl=True,
    )

    def __init__(self, store_path: str = ":memory:", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.store_path = store_path
        self._connection: _OxigraphConnectionWrapper | None = None
        self._lock = asyncio.Lock()

    # ---- lifecycle --------------------------------------------------------

    async def create_connection(self, with_db: bool) -> None:
        if self._connection is not None:
            return
        loop = asyncio.get_running_loop()
        if self.store_path == ":memory:":
            store = ox.Store()
        else:
            store = ox.Store(path=self.store_path)
        self._connection = _OxigraphConnectionWrapper(store, loop)
        await self._post_connect()
        log.debug("Opened oxigraph store: %s", self.store_path)

    async def close(self) -> None:
        if self._connection is not None:
            try:
                self._connection.store.flush()
            except Exception:
                pass
            self._connection = None
        log.debug("Closed oxigraph store: %s", self.store_path)

    async def db_create(self) -> None:
        pass   # store is created in create_connection

    async def db_delete(self) -> None:
        await self.close()
        if self.store_path != ":memory:":
            import shutil, os
            if os.path.exists(self.store_path):
                shutil.rmtree(self.store_path)

    # ---- connection acquisition -------------------------------------------

    def acquire_connection(self) -> _OxigraphConnectionContextManager:
        return _OxigraphConnectionContextManager(self)

    def _in_transaction(self) -> _OxigraphTransactionContext:
        wrapper = OxigraphTransactionWrapper(self)
        wrapper.executor_class = self.__class__.executor_class
        wrapper.schema_generator = self.__class__.schema_generator
        wrapper.capabilities = self.__class__.capabilities
        return _OxigraphTransactionContext(wrapper)

    # ---- direct store access (used by executor) ---------------------------

    async def ensure_connected(self) -> None:
        """Lazily establish the store connection if not yet open."""
        if self._connection is None:
            await self.create_connection(with_db=True)

    @property
    def store(self) -> ox.Store:
        if self._connection is None:
            raise OperationalError("Not connected – call create_connection() first")
        return self._connection.store

    async def run_in_executor(self, fn, *args, **kwargs):
        """Run a synchronous callable in the thread-pool executor."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    # ---- SQL pass-through (execute_query receives pre-built SQL) ----------
    # The executor overrides the high-level methods so these are only called
    # for schema generation (which emits no-op DDL from our schema generator).

    async def execute_insert(self, query: str, values: list) -> Any:
        # Only reached when executing INSERT SQL (schema ops); no-op for oxigraph.
        log.debug("execute_insert (no-op SQL): %s", query)
        return None

    async def execute_query(
        self, query: str, values: list | None = None
    ) -> tuple[int, Sequence[dict]]:
        """
        Receives a SQL SELECT / UPDATE / DELETE string from the base executor.

        For SELECT we attempt a SQL→SPARQL translation.
        For UPDATE / DELETE this is a no-op because our executor overrides those
        at a higher level and never reaches here.
        """
        await self.ensure_connected()
        q = query.strip().upper()
        if q.startswith("SELECT"):
            return await self._execute_select_sql(query, values)
        log.debug("execute_query (no-op): %s", query)
        return 0, []

    async def _execute_select_sql(
        self, sql: str, values: list | None
    ) -> tuple[int, Sequence[dict]]:
        """Translate a SELECT SQL string + values into SPARQL and run it."""
        from tortoise_oxigraph.filters import parse_select
        from tortoise_oxigraph.sparql_builder import build_sparql_select
        parsed = parse_select(sql, values)
        sparql = build_sparql_select(parsed, self._table_to_model_meta)
        log.debug("SPARQL SELECT: %s", sparql)
        rows = await self.run_in_executor(self._run_sparql_select, sparql)
        return len(rows), rows

    def _run_sparql_select(self, sparql: str) -> list[dict]:
        results = self.store.query(sparql)
        var_names = [v.value for v in results.variables]
        rows: list[dict] = []
        from tortoise_oxigraph.rdf_utils import term_to_python
        for solution in results:
            row = {}
            for var in var_names:
                try:
                    val = solution[var]
                    row[var] = term_to_python(val) if val is not None else None
                except Exception:
                    row[var] = None
            rows.append(row)
        return rows

    async def execute_script(self, query: str) -> None:
        log.debug("execute_script (no-op): %s", query[:80])

    async def execute_many(self, query: str, values: list[list]) -> None:
        """Handle bulk INSERT SQL by translating each row to RDF triples."""
        await self.ensure_connected()
        q = query.strip().upper()
        if not q.startswith("INSERT"):
            log.debug("execute_many (non-INSERT, no-op): %s", query[:80])
            return
        await self._execute_bulk_insert_sql(query, values)

    async def _execute_bulk_insert_sql(self, sql: str, rows: list[list]) -> None:
        """
        Parse an INSERT SQL statement and persist each row as RDF triples.

        SQL form:  INSERT INTO "table" ("col1","col2") VALUES (?,?)
        """
        import re
        from tortoise_oxigraph.rdf_utils import (
            _RDF_TYPE, field_predicate_iri, model_class_iri,
            model_instance_iri, next_sequence, python_to_term,
        )

        m = re.search(r'INSERT\s+INTO\s+"?(\w+)"?', sql, re.IGNORECASE)
        if not m:
            log.warning("execute_many: cannot parse table from: %s", sql[:80])
            return
        table = m.group(1)

        col_m = re.search(r'\(([^)]+)\)\s+VALUES', sql, re.IGNORECASE)
        if not col_m:
            log.warning("execute_many: cannot parse columns from: %s", sql[:80])
            return
        cols = [c.strip().strip('"') for c in col_m.group(1).split(",")]

        meta = self._table_to_model_meta.get(table)
        if meta is None:
            log.warning("execute_many: no meta for table %s", table)
            return

        try:
            app = meta.app or "default"
        except AttributeError:
            app = "default"
        model_name = meta._model.__name__
        type_iri = model_class_iri(app, model_name)
        pk_col = meta.db_pk_column

        def _insert_row(row_values: list) -> None:
            row_dict = dict(zip(cols, row_values))
            pk_val = row_dict.get(pk_col)
            if pk_val is None:
                pk_val = next_sequence(self.store, app, model_name)
                row_dict[pk_col] = pk_val

            subj = model_instance_iri(app, model_name, pk_val)
            quads = [ox.Quad(subj, _RDF_TYPE, type_iri, ox.DefaultGraph())]
            for col, val in row_dict.items():
                if val is None:
                    continue
                try:
                    term = python_to_term(val)
                except Exception:
                    term = ox.Literal(str(val))
                pred = field_predicate_iri(app, model_name, col)
                quads.append(ox.Quad(subj, pred, term, ox.DefaultGraph()))
            self.store.extend(quads)

        await self.run_in_executor(lambda: [_insert_row(r) for r in rows])

    async def execute_query_dict(
        self, query: str, values: list | None = None
    ) -> list[dict]:
        _, rows = await self.execute_query(query, values)
        return list(rows)

    async def execute_query_dict_with_affected(
        self, query: str, values: list | None = None
    ) -> tuple[list[dict], int]:
        count, rows = await self.execute_query(query, values)
        return list(rows), count

    # ---- model metadata registry ------------------------------------------
    # Populated by the executor on first use so SQL→SPARQL can resolve tables.

    _table_to_model_meta: dict[str, Any] = {}

    def register_model_meta(self, table: str, meta: Any) -> None:
        self._table_to_model_meta[table] = meta
