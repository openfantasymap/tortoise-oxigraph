"""
Build SPARQL SELECT queries from a :class:`ParsedSelect` description.

Each model instance is stored as a set of triples:

  <tortoise://app/Model/pk>  rdf:type           <tortoise://app/Model>
  <tortoise://app/Model/pk>  <tortoise://app/Model#field>  value_literal

The SPARQL SELECT pattern is:

  SELECT ?col1 ?col2 …
  WHERE {
    ?s rdf:type <tortoise://app/Model> .
    OPTIONAL { ?s <…#col1> ?col1 }
    OPTIONAL { ?s <…#col2> ?col2 }
    FILTER (…conditions…)
  }
  ORDER BY ASC(?col)
  LIMIT n OFFSET m
"""

from __future__ import annotations

from typing import Any

from tortoise_oxigraph.filters import Condition, ParsedSelect, like_to_regex
from tortoise_oxigraph.rdf_utils import (
    _NS,
    _RDF_TYPE,
    field_predicate_iri,
    model_class_iri,
    python_to_term,
)


def build_sparql_select(parsed: ParsedSelect, table_meta: dict[str, Any]) -> str:
    """
    Convert a :class:`ParsedSelect` into a SPARQL SELECT string.

    ``table_meta`` maps table-name → Tortoise model _meta object,
    used to resolve the app-name and field names.
    """
    meta = table_meta.get(parsed.table)
    if meta is None:
        # Unknown table – return an empty result set query
        return "SELECT * WHERE { FILTER(false) }"

    app = _app_name(meta)
    model_name = meta.db_table  # we use db_table as the "model_name" in IRIs
    # Use the actual class name for the type IRI
    actual_model_name = meta._model.__name__
    type_iri = model_class_iri(app, actual_model_name)

    # Determine which columns to project
    if parsed.is_count:
        return _build_count_sparql(type_iri)

    all_columns = _all_db_columns(meta)
    project_cols = (
        [c for c in parsed.columns if c in all_columns]
        if parsed.columns and parsed.columns != ["*"]
        else all_columns
    )

    # Build SPARQL
    lines: list[str] = []

    # SELECT clause
    select_vars = " ".join(f"?{_safe_var(c)}" for c in ["_subject"] + project_cols)
    lines.append(f"SELECT {select_vars}")
    lines.append("WHERE {")
    lines.append(f"  ?_subject <{_RDF_TYPE.value}> <{type_iri.value}> .")

    # OPTIONAL field bindings
    for col in project_cols:
        pred = field_predicate_iri(app, actual_model_name, col)
        var = _safe_var(col)
        lines.append(f"  OPTIONAL {{ ?_subject <{pred.value}> ?{var} }}")

    # FILTER conditions
    filter_expr = _build_filter(parsed.conditions, app, actual_model_name)
    if filter_expr:
        lines.append(f"  FILTER ({filter_expr})")

    lines.append("}")

    # ORDER BY
    if parsed.order_by:
        order_parts = []
        for col, direction in parsed.order_by:
            var = _safe_var(col)
            if direction == "DESC":
                order_parts.append(f"DESC(?{var})")
            else:
                order_parts.append(f"ASC(?{var})")
        lines.append("ORDER BY " + " ".join(order_parts))

    # LIMIT / OFFSET
    if parsed.limit is not None:
        lines.append(f"LIMIT {parsed.limit}")
    if parsed.offset is not None:
        lines.append(f"OFFSET {parsed.offset}")

    return "\n".join(lines)


def _build_count_sparql(type_iri) -> str:
    return (
        f"SELECT (COUNT(?s) AS ?count)\n"
        f"WHERE {{\n"
        f"  ?s <{_RDF_TYPE.value}> <{type_iri.value}> .\n"
        f"}}"
    )


def _build_filter(conditions: list[Condition], app: str, model_name: str) -> str:
    if not conditions:
        return ""

    parts: list[str] = []
    for i, cond in enumerate(conditions):
        connector = cond.connector if i > 0 else ""
        expr = _condition_to_sparql(cond, app, model_name)
        if expr:
            if connector:
                parts.append(f" {connector} ")
            parts.append(expr)

    return "".join(parts)


def _condition_to_sparql(cond: Condition, app: str, model_name: str) -> str:
    var = f"?{_safe_var(cond.column)}"

    if cond.op == "IS NULL":
        return f"!BOUND({var})"

    if cond.op == "IS NOT NULL":
        return f"BOUND({var})"

    if cond.op in ("=", "!=", "<", ">", "<=", ">="):
        rhs = _sparql_literal(cond.value)
        sparql_op = "!=" if cond.op == "!=" else cond.op
        return f"{var} {sparql_op} {rhs}"

    if cond.op in ("LIKE", "NOT LIKE"):
        pattern = like_to_regex(str(cond.value))
        flags = "" if cond.op == "LIKE" else ""
        expr = f'REGEX(STR({var}), "{pattern}")'
        return f"!({expr})" if "NOT" in cond.op else expr

    if cond.op in ("IN", "NOT IN"):
        vals = " ".join(_sparql_literal(v) for v in cond.value)
        expr = f"{var} IN ({vals})"
        return f"!({expr})" if "NOT IN" == cond.op else expr

    if cond.op == "BETWEEN":
        lo, hi = cond.value
        return f"{var} >= {_sparql_literal(lo)} && {var} <= {_sparql_literal(hi)}"

    return ""


def _sparql_literal(value: Any) -> str:
    """Convert a Python value to a SPARQL inline literal."""
    if value is None:
        return "UNDEF"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    from decimal import Decimal
    if isinstance(value, Decimal):
        return str(value)
    # String – escape quotes
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _safe_var(col: str) -> str:
    """Return a SPARQL-safe variable name from a column name."""
    return col.replace("-", "_").replace(".", "_")


def _app_name(meta: Any) -> str:
    """Extract app name from tortoise model meta."""
    try:
        return meta.app
    except AttributeError:
        return "default"


def _all_db_columns(meta: Any) -> list[str]:
    """Return the list of all DB column names for a model."""
    try:
        return list(meta.fields_db_projection.values())
    except AttributeError:
        return []
