"""
User-defined schema config for the publishers table.

Derived from schema detection on the Mar 2026 snapshot.

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)
"""

PUBLISHERS_SCHEMA = {
    # ── Scalar fields ────────────────────────────────────────────────────────
    "id":                           {"type": "str"},
    "display_name":                 {"type": "str"},
    "homepage_url":                 {"type": "str"},
    "image_url":                    {"type": "str"},
    "image_thumbnail_url":          {"type": "str"},
    "ror_id":                       {"type": "str"},
    "wikidata_id":                  {"type": "str"},
    "hierarchy_level":              {"type": "int"},
    "works_count":                  {"type": "int"},
    "cited_by_count":               {"type": "int"},
    "sources_api_url":              {"type": "str"},
    "created_date":                 {"type": "str"},
    "updated_date":                 {"type": "str"},

    # ── Nested objects (stored as JSON string) ───────────────────────────────
    "ids":                          {"type": "json"},
    "summary_stats":                {"type": "json"},
    "parent_publisher":             {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "alternate_titles":             {"type": "json"},
    "country_codes":                {"type": "json"},
    "lineage":                      {"type": "json"},
    "roles":                        {"type": "json"},
    "counts_by_year":               {"type": "json"},
}
