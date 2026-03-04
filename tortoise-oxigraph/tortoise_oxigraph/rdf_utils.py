"""
RDF namespace helpers, IRI construction, and Python↔RDF type mapping.

Data model
----------
Each Tortoise model instance becomes a set of triples in the default graph:

  subject   = <tortoise://app/Model/pk_value>
  predicate = <tortoise://app/Model#field_name>
  object    = typed Literal  (or NamedNode for FK references)

A special rdf:type triple marks each subject with its model class:

  <tortoise://app/Model/pk> <rdf:type> <tortoise://app/Model>
"""

from __future__ import annotations

import datetime
import uuid
from decimal import Decimal
from typing import Any

import pyoxigraph as ox

# ---------------------------------------------------------------------------
# Namespace constants
# ---------------------------------------------------------------------------

_NS = "tortoise://"
_XSD = "http://www.w3.org/2001/XMLSchema#"
_RDF_TYPE = ox.NamedNode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")

# XSD type IRIs used for Literal creation
XSD_STRING   = ox.NamedNode(_XSD + "string")
XSD_INTEGER  = ox.NamedNode(_XSD + "integer")
XSD_LONG     = ox.NamedNode(_XSD + "long")
XSD_FLOAT    = ox.NamedNode(_XSD + "float")
XSD_DOUBLE   = ox.NamedNode(_XSD + "double")
XSD_DECIMAL  = ox.NamedNode(_XSD + "decimal")
XSD_BOOLEAN  = ox.NamedNode(_XSD + "boolean")
XSD_DATETIME = ox.NamedNode(_XSD + "dateTime")
XSD_DATE     = ox.NamedNode(_XSD + "date")
XSD_TIME     = ox.NamedNode(_XSD + "time")
XSD_BYTES    = ox.NamedNode(_XSD + "base64Binary")
XSD_UUID     = ox.NamedNode(_NS + "uuid")   # custom extension

# Sequence predicate – used to store the auto-increment counter per model.
#   <tortoise://__seq#app/Model> <tortoise://__seq#value> "n"^^xsd:long
_SEQ_SUBJECT_PREFIX = f"{_NS}__seq#"
_SEQ_VALUE_PRED = ox.NamedNode(f"{_NS}__seq#value")


# ---------------------------------------------------------------------------
# IRI helpers
# ---------------------------------------------------------------------------

def model_class_iri(app: str, model_name: str) -> ox.NamedNode:
    """IRI for a model class – used as rdf:type object."""
    return ox.NamedNode(f"{_NS}{app}/{model_name}")


def model_instance_iri(app: str, model_name: str, pk: Any) -> ox.NamedNode:
    """IRI for a specific model instance."""
    return ox.NamedNode(f"{_NS}{app}/{model_name}/{pk}")


def field_predicate_iri(app: str, model_name: str, field_name: str) -> ox.NamedNode:
    """IRI for a model field predicate."""
    return ox.NamedNode(f"{_NS}{app}/{model_name}#{field_name}")


def seq_subject_iri(app: str, model_name: str) -> ox.NamedNode:
    """IRI for the auto-increment sequence counter of a model."""
    return ox.NamedNode(f"{_SEQ_SUBJECT_PREFIX}{app}/{model_name}")


# ---------------------------------------------------------------------------
# Python → RDF Literal
# ---------------------------------------------------------------------------

def python_to_term(value: Any) -> ox.NamedNode | ox.Literal:
    """Convert a Python value to an RDF term (Literal or NamedNode)."""
    if value is None:
        raise ValueError("None cannot be represented as an RDF term (skip the triple instead)")

    if isinstance(value, bool):
        return ox.Literal("true" if value else "false", datatype=XSD_BOOLEAN)

    if isinstance(value, int):
        return ox.Literal(str(value), datatype=XSD_LONG)

    if isinstance(value, float):
        return ox.Literal(repr(value), datatype=XSD_DOUBLE)

    if isinstance(value, Decimal):
        return ox.Literal(str(value), datatype=XSD_DECIMAL)

    if isinstance(value, datetime.datetime):
        return ox.Literal(value.isoformat(), datatype=XSD_DATETIME)

    if isinstance(value, datetime.date):
        return ox.Literal(value.isoformat(), datatype=XSD_DATE)

    if isinstance(value, datetime.time):
        return ox.Literal(value.isoformat(), datatype=XSD_TIME)

    if isinstance(value, uuid.UUID):
        return ox.Literal(str(value), datatype=XSD_UUID)

    if isinstance(value, bytes):
        import base64
        return ox.Literal(base64.b64encode(value).decode(), datatype=XSD_BYTES)

    # Fallback: plain string
    return ox.Literal(str(value), datatype=XSD_STRING)


# ---------------------------------------------------------------------------
# RDF Literal → Python
# ---------------------------------------------------------------------------

def term_to_python(term: ox.NamedNode | ox.Literal | ox.BlankNode) -> Any:
    """Convert an RDF term back to a Python value."""
    if isinstance(term, ox.NamedNode):
        return str(term.value)

    if isinstance(term, ox.BlankNode):
        return str(term.value)

    # It's a Literal
    dt = term.datatype.value if term.datatype else None
    raw = term.value

    if dt is None or dt == XSD_STRING.value:
        return raw

    if dt == XSD_BOOLEAN.value:
        return raw.lower() in ("true", "1", "yes")

    if dt in (XSD_INTEGER.value, XSD_LONG.value):
        return int(raw)

    if dt == XSD_FLOAT.value:
        return float(raw)

    if dt == XSD_DOUBLE.value:
        return float(raw)

    if dt == XSD_DECIMAL.value:
        return Decimal(raw)

    if dt == XSD_DATETIME.value:
        return datetime.datetime.fromisoformat(raw)

    if dt == XSD_DATE.value:
        return datetime.date.fromisoformat(raw)

    if dt == XSD_TIME.value:
        return datetime.time.fromisoformat(raw)

    if dt == XSD_UUID.value:
        return uuid.UUID(raw)

    if dt == XSD_BYTES.value:
        import base64
        return base64.b64decode(raw.encode())

    # Unknown datatype – return raw string
    return raw


# ---------------------------------------------------------------------------
# Sequence (auto-increment) helpers – operate directly on the Store
# ---------------------------------------------------------------------------

def read_sequence(store: ox.Store, app: str, model_name: str) -> int:
    """Return the current sequence value (0 if not yet initialised)."""
    subj = seq_subject_iri(app, model_name)
    for quad in store.quads_for_pattern(subj, _SEQ_VALUE_PRED, None, None):
        return int(quad.object.value)
    return 0


def next_sequence(store: ox.Store, app: str, model_name: str) -> int:
    """Atomically increment and return the next sequence value."""
    subj = seq_subject_iri(app, model_name)
    current = read_sequence(store, app, model_name)
    nxt = current + 1
    # Remove old value, insert new
    store.remove(ox.Quad(subj, _SEQ_VALUE_PRED, ox.Literal(str(current), datatype=XSD_LONG), ox.DefaultGraph()))
    store.add(ox.Quad(subj, _SEQ_VALUE_PRED, ox.Literal(str(nxt), datatype=XSD_LONG), ox.DefaultGraph()))
    return nxt
