"""
User-defined schema config for the authors table.

Union of all fields across snapshots:
  - Nov 2025 (data/authors)
  - Mar 2026 (data/authors)

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)

Fields that only exist in one snapshot will be null in the other.
"""

AUTHORS_SCHEMA = {
    # ── Scalar fields ────────────────────────────────────────────────────────
    "id":                           {"type": "str"},
    "orcid":                        {"type": "str"},
    "display_name":                 {"type": "str"},
    "works_count":                  {"type": "int"},
    "cited_by_count":               {"type": "int"},
    "created_date":                 {"type": "str"},
    "updated_date":                 {"type": "str"},
    "works_api_url":                {"type": "str"},

    # ── Nested objects (stored as JSON string) ───────────────────────────────
    "ids":                          {"type": "json"},
    "summary_stats":                {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "display_name_alternatives":    {"type": "json"},
    "affiliations":                 {"type": "json"},
    "last_known_institutions":      {"type": "json"},
    "sources":                      {"type": "json"},
    "topics":                       {"type": "json"},
    "topic_share":                  {"type": "json"},
    "x_concepts":                   {"type": "json"},
    "counts_by_year":               {"type": "json"},
}
