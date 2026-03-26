"""
User-defined schema config for the funders table.

Derived from schema detection on the Mar 2026 snapshot.

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)
"""

FUNDERS_SCHEMA = {
    # ── Scalar fields ────────────────────────────────────────────────────────
    "id":                           {"type": "str"},
    "display_name":                 {"type": "str"},
    "description":                  {"type": "str"},
    "homepage_url":                 {"type": "str"},
    "image_url":                    {"type": "str"},
    "image_thumbnail_url":          {"type": "str"},
    "country_code":                 {"type": "str"},
    "awards_count":                 {"type": "int"},
    "works_count":                  {"type": "int"},
    "cited_by_count":               {"type": "int"},
    "created_date":                 {"type": "str"},
    "updated_date":                 {"type": "str"},

    # ── Nested objects (stored as JSON string) ───────────────────────────────
    "ids":                          {"type": "json"},
    "summary_stats":                {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "alternate_titles":             {"type": "json"},
    "roles":                        {"type": "json"},
    "counts_by_year":               {"type": "json"},
}
