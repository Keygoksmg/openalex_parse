"""
User-defined schema config for the institutions table.

Union of all fields across snapshots:
  - Nov 2025 (data/institutions)
  - Mar 2026 (data/institutions)

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)

Fields that only exist in one snapshot will be null in the other.
"""

INSTITUTIONS_SCHEMA = {
    # ── Scalar fields ────────────────────────────────────────────────────────
    "id":                           {"type": "str"},
    "ror":                          {"type": "str"},
    "display_name":                 {"type": "str"},
    "country_code":                 {"type": "str"},
    "type":                         {"type": "str"},
    "type_id":                      {"type": "str"},
    "homepage_url":                 {"type": "str"},
    "image_url":                    {"type": "str"},
    "image_thumbnail_url":          {"type": "str"},
    "works_count":                  {"type": "int"},
    "cited_by_count":               {"type": "int"},
    "is_super_system":              {"type": "bool"},
    "wiki_page":                    {"type": "str"},      # Nov 2025 only
    "wikidata_id":                  {"type": "str"},      # Nov 2025 only
    "created_date":                 {"type": "str"},
    "updated_date":                 {"type": "str"},
    "works_api_url":                {"type": "str"},

    # ── Nested objects (stored as JSON string) ───────────────────────────────
    "ids":                          {"type": "json"},
    "geo":                          {"type": "json"},
    "summary_stats":                {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "display_name_acronyms":        {"type": "json"},
    "display_name_alternatives":    {"type": "json"},
    "associated_institutions":      {"type": "json"},
    "lineage":                      {"type": "json"},
    "repositories":                 {"type": "json"},
    "roles":                        {"type": "json"},
    "topics":                       {"type": "json"},
    "topic_share":                  {"type": "json"},
    "counts_by_year":               {"type": "json"},
}
