"""
User-defined schema config for the continents table.

Derived from schema detection on the Mar 2026 snapshot.

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)
"""

CONTINENTS_SCHEMA = {
    # ── Scalar fields ────────────────────────────────────────────────────────
    "id":                           {"type": "str"},
    "display_name":                 {"type": "str"},
    "description":                  {"type": "str"},
    "wikidata_id":                  {"type": "str"},
    "wikidata_url":                 {"type": "str"},
    "wikipedia_url":                {"type": "str"},
    "created_date":                 {"type": "str"},
    "updated_date":                 {"type": "str"},

    # ── Nested objects (stored as JSON string) ───────────────────────────────
    "ids":                          {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "display_name_alternatives":    {"type": "json"},
    "countries":                    {"type": "json"},
}
