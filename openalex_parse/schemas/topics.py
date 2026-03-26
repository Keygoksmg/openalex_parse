"""
User-defined schema config for the topics table.

Derived from schema detection on the Mar 2026 snapshot.

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)
"""

TOPICS_SCHEMA = {
    # ── Scalar fields ────────────────────────────────────────────────────────
    "id":                           {"type": "str"},
    "display_name":                 {"type": "str"},
    "description":                  {"type": "str"},
    "works_count":                  {"type": "int"},
    "cited_by_count":               {"type": "int"},
    "created_date":                 {"type": "str"},
    "updated_date":                 {"type": "str"},
    "works_api_url":                {"type": "str"},

    # ── Nested objects (stored as JSON string) ───────────────────────────────
    "ids":                          {"type": "json"},
    "domain":                       {"type": "json"},
    "field":                        {"type": "json"},
    "subfield":                     {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "keywords":                     {"type": "json"},
    "siblings":                     {"type": "json"},
}
