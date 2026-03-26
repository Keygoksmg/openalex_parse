"""
User-defined schema config for the countries table.

Derived from schema detection on the Mar 2026 snapshot.

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)
"""

COUNTRIES_SCHEMA = {
    # ── Scalar fields ────────────────────────────────────────────────────────
    "id":                           {"type": "str"},
    "display_name":                 {"type": "str"},
    "country_code":                 {"type": "str"},
    "alpha_3":                      {"type": "str"},
    "full_name":                    {"type": "str"},
    "description":                  {"type": "str"},
    "is_global_south":              {"type": "bool"},
    "numeric":                      {"type": "int"},
    "continent_id":                 {"type": "int"},
    "authors_api_url":              {"type": "str"},
    "institutions_api_url":         {"type": "str"},
    "wikidata_url":                 {"type": "str"},
    "wikipedia_url":                {"type": "str"},
    "created_date":                 {"type": "str"},
    "updated_date":                 {"type": "str"},
    "works_api_url":                {"type": "str"},
    "works_count":                  {"type": "int"},
    "cited_by_count":               {"type": "int"},

    # ── Nested objects (stored as JSON string) ───────────────────────────────
    "ids":                          {"type": "json"},
    "continent":                    {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "display_name_alternatives":    {"type": "json"},
}
