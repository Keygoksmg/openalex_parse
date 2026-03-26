"""
User-defined schema config for the sources table.

Derived from schema detection on the Mar 2026 snapshot.

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)
"""

SOURCES_SCHEMA = {
    # ── Scalar fields ────────────────────────────────────────────────────────
    "id":                           {"type": "str"},
    "display_name":                 {"type": "str"},
    "issn_l":                       {"type": "str"},
    "type":                         {"type": "str"},
    "country_code":                 {"type": "str"},
    "homepage_url":                 {"type": "str"},
    "host_organization":            {"type": "str"},
    "host_organization_name":       {"type": "str"},
    "is_oa":                        {"type": "bool"},
    "is_in_doaj":                   {"type": "bool"},
    "is_in_doaj_since_year":        {"type": "int"},
    "is_in_scielo":                 {"type": "bool"},
    "is_core":                      {"type": "bool"},
    "is_ojs":                       {"type": "bool"},
    "is_high_oa_rate":              {"type": "bool"},
    "is_high_oa_rate_since_year":   {"type": "int"},
    "first_publication_year":       {"type": "int"},
    "last_publication_year":        {"type": "int"},
    "oa_flip_year":                 {"type": "int"},
    "oa_works_count":               {"type": "int"},
    "apc_usd":                      {"type": "int"},
    "works_count":                  {"type": "int"},
    "cited_by_count":               {"type": "int"},
    "created_date":                 {"type": "str"},
    "updated_date":                 {"type": "str"},
    "works_api_url":                {"type": "str"},

    # ── Nested objects (stored as JSON string) ───────────────────────────────
    "ids":                          {"type": "json"},
    "summary_stats":                {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "alternate_titles":             {"type": "json"},
    "issn":                         {"type": "json"},
    "host_organization_lineage":    {"type": "json"},
    "apc_prices":                   {"type": "json"},
    "societies":                    {"type": "json"},
    "topics":                       {"type": "json"},
    "topic_share":                  {"type": "json"},
    "counts_by_year":               {"type": "json"},
}
