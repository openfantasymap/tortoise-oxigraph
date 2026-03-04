"""
Translate the SQL WHERE clause emitted by pypika-tortoise into a SPARQL
FILTER / graph-pattern fragment.

pypika-tortoise generates SQL that looks like:

  SELECT "t"."id","t"."name" FROM "myapp_tournament" "t"
  WHERE "t"."id"=? AND "t"."name" LIKE ?
  ORDER BY "t"."id" ASC LIMIT 100 OFFSET 0

This module extracts:
  * table / alias name
  * selected columns
  * WHERE conditions
  * ORDER BY column + direction
  * LIMIT / OFFSET
  * whether it's a COUNT query
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Parsed query representation
# ---------------------------------------------------------------------------

@dataclass
class ParsedSelect:
    table: str                          # DB table name (unquoted)
    columns: list[str]                  # unquoted column names, or ["*"] / ["COUNT"]
    is_count: bool = False              # True if SELECT COUNT(…)
    conditions: list["Condition"] = field(default_factory=list)
    order_by: list[tuple[str, str]] = field(default_factory=list)  # (col, "ASC"|"DESC")
    limit: int | None = None
    offset: int | None = None


@dataclass
class Condition:
    """A single WHERE predicate."""
    column: str         # unquoted column name
    op: str             # "=", "!=", "<", ">", "<=", ">=", "LIKE", "NOT LIKE",
                        # "IS NULL", "IS NOT NULL", "IN", "NOT IN", "BETWEEN"
    value: Any = None   # scalar value, list (IN), or tuple (BETWEEN)
    connector: str = "AND"   # "AND" | "OR" (connector before this condition)


# ---------------------------------------------------------------------------
# Regex helpers
# ---------------------------------------------------------------------------

# Strip double-quote quoting from SQL identifiers  "foo" → foo
_UNQUOTE = re.compile(r'"([^"]+)"')

def _unquote(s: str) -> str:
    m = _UNQUOTE.fullmatch(s.strip())
    return m.group(1) if m else s.strip()

def _strip_alias(col: str, alias: str) -> str:
    """Remove table alias prefix:  "t"."name" → name"""
    col = col.strip()
    # "alias"."col"  or  alias."col"  or  "col"
    m = re.match(r'^(?:"[^"]+"|[A-Za-z_]\w*)\."([^"]+)"$', col)
    if m:
        return m.group(1)
    return _unquote(col)


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

_SELECT_RE = re.compile(
    r'^\s*SELECT\s+(.+?)\s+FROM\s+"?(\w+)"?\s*(?:"?\w+"?)?\s*'
    r'(?:WHERE\s+(.+?))?'
    r'(?:ORDER BY\s+(.+?))?'
    r'(?:LIMIT\s+(\d+|\?))?'
    r'(?:\s+OFFSET\s+(\d+|\?))?'
    r'\s*$',
    re.IGNORECASE | re.DOTALL,
)

# Tokenise WHERE clause into atoms separated by AND/OR
_COND_SPLIT = re.compile(
    r'\s+(AND|OR)\s+',
    re.IGNORECASE,
)

# Individual condition patterns
_EQ_RE        = re.compile(r'^(.+?)\s*(=|!=|<>|<=|>=|<|>)\s*(\?|[^\s]+)$')
_LIKE_RE      = re.compile(r'^(.+?)\s+(NOT\s+LIKE|LIKE)\s+(\?|[^\s]+)', re.IGNORECASE)
_NULL_RE      = re.compile(r'^(.+?)\s+IS\s+(NOT\s+NULL|NULL)$', re.IGNORECASE)
_IN_RE        = re.compile(r'^(.+?)\s+(NOT\s+IN|IN)\s*\(([^)]+)\)$', re.IGNORECASE)
_BETWEEN_RE   = re.compile(r'^(.+?)\s+BETWEEN\s+(\?|[^\s]+)\s+AND\s+(\?|[^\s]+)$', re.IGNORECASE)

# COUNT detection
_COUNT_RE     = re.compile(r'COUNT\s*\(', re.IGNORECASE)


def parse_select(sql: str, values: list[Any] | None = None) -> ParsedSelect:
    """
    Parse a pypika-tortoise SELECT statement into a :class:`ParsedSelect`.

    ``values`` is the list of positional ``?`` parameters in the same order
    they appear in the SQL.
    """
    values = list(values or [])
    placeholder_idx = [0]   # mutable counter

    def next_val() -> Any:
        idx = placeholder_idx[0]
        if idx < len(values):
            placeholder_idx[0] += 1
            return values[idx]
        return None

    # ---- match top-level structure ----------------------------------------
    m = _SELECT_RE.match(sql.strip())
    if not m:
        # Fallback: return a full-scan description so the caller can handle it
        return ParsedSelect(table="__unknown__", columns=["*"])

    select_part, table_raw, where_part, order_part, limit_raw, offset_raw = m.groups()

    table = _unquote(table_raw)

    # ---- columns -----------------------------------------------------------
    is_count = bool(_COUNT_RE.search(select_part))
    if is_count:
        columns = ["COUNT"]
    else:
        columns = [_strip_alias(c, "") for c in select_part.split(",")]

    # ---- WHERE conditions (must come before LIMIT/OFFSET to consume ? in order) --
    conditions: list[Condition] = []
    if where_part:
        # Split by AND/OR to get raw atoms + connectors
        atoms = _COND_SPLIT.split(where_part.strip())
        # atoms = [expr, "AND"|"OR", expr, "AND"|"OR", expr, ...]
        connector = "AND"
        for atom in atoms:
            upper = atom.strip().upper()
            if upper in ("AND", "OR"):
                connector = upper
                continue
            cond = _parse_condition(atom.strip(), next_val, connector)
            if cond:
                conditions.append(cond)
            connector = "AND"   # reset for next

    # ---- ORDER BY ----------------------------------------------------------
    order_by: list[tuple[str, str]] = []
    if order_part:
        for item in order_part.split(","):
            parts = item.strip().split()
            col = _strip_alias(parts[0], "")
            direction = parts[1].upper() if len(parts) > 1 else "ASC"
            order_by.append((col, direction))

    # ---- LIMIT / OFFSET (consume ? values after WHERE) ---------------------
    limit: int | None = None
    offset: int | None = None
    if limit_raw:
        limit = int(next_val()) if limit_raw == "?" else int(limit_raw)
    if offset_raw:
        offset = int(next_val()) if offset_raw == "?" else int(offset_raw)

    return ParsedSelect(
        table=table,
        columns=columns,
        is_count=is_count,
        conditions=conditions,
        order_by=order_by,
        limit=limit,
        offset=offset,
    )


def _parse_condition(expr: str, next_val, connector: str) -> Condition | None:
    """Parse a single WHERE atom into a Condition."""

    # IS NULL / IS NOT NULL
    m = _NULL_RE.match(expr)
    if m:
        col = _strip_alias(m.group(1), "")
        op = "IS NOT NULL" if "NOT" in m.group(2).upper() else "IS NULL"
        return Condition(column=col, op=op, connector=connector)

    # BETWEEN
    m = _BETWEEN_RE.match(expr)
    if m:
        col = _strip_alias(m.group(1), "")
        lo = next_val() if m.group(2) == "?" else m.group(2)
        hi = next_val() if m.group(3) == "?" else m.group(3)
        return Condition(column=col, op="BETWEEN", value=(lo, hi), connector=connector)

    # IN / NOT IN
    m = _IN_RE.match(expr)
    if m:
        col = _strip_alias(m.group(1), "")
        op = "NOT IN" if "NOT" in m.group(2).upper() else "IN"
        # Each placeholder in the list
        placeholders = [p.strip() for p in m.group(3).split(",")]
        vals = [next_val() if p == "?" else p for p in placeholders]
        return Condition(column=col, op=op, value=vals, connector=connector)

    # LIKE / NOT LIKE
    m = _LIKE_RE.match(expr)
    if m:
        col = _strip_alias(m.group(1), "")
        op = "NOT LIKE" if "NOT" in m.group(2).upper() else "LIKE"
        val = next_val() if m.group(3) == "?" else m.group(3)
        return Condition(column=col, op=op, value=val, connector=connector)

    # Comparison: =, !=, <>, <, >, <=, >=
    m = _EQ_RE.match(expr)
    if m:
        col = _strip_alias(m.group(1), "")
        op = m.group(2).replace("<>", "!=")
        raw = m.group(3)
        val = next_val() if raw == "?" else raw
        return Condition(column=col, op=op, value=val, connector=connector)

    return None


# ---------------------------------------------------------------------------
# LIKE pattern → SPARQL REGEX pattern
# ---------------------------------------------------------------------------

def like_to_regex(pattern: str, case_sensitive: bool = True) -> str:
    """Convert a SQL LIKE pattern (with % and _) to a SPARQL-compatible regex."""
    import re as _re
    # Escape regex special chars first (except the SQL wildcards we'll handle)
    escaped = _re.sub(r'([.^$*+?{}[\]|()])', r'\\\1', str(pattern))
    # SQL wildcards: % → .* and _ → .
    escaped = escaped.replace("%", ".*").replace("_", ".")
    return f"^{escaped}$"
