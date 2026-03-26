"""
User-defined schema config for the concepts table.

Derived from schema detection on the Nov 2025 snapshot (updated_date=2022-04-12).
Note: OpenAlex deprecated concepts in favor of topics. This schema is retained
for backward compatibility.

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)
"""

CONCEPTS_SCHEMA = {
    # -- Scalar fields --
    "id":                   {"type": "str"},
    "wikidata":             {"type": "str"},
    "display_name":         {"type": "str"},
    "level":                {"type": "int"},
    "description":          {"type": "str"},
    "works_count":          {"type": "int"},
    "cited_by_count":       {"type": "int"},
    "image_url":            {"type": "str"},
    "image_thumbnail_url":  {"type": "str"},
    "created_date":         {"type": "str"},
    "updated_date":         {"type": "str"},
    "works_api_url":        {"type": "str"},

    # -- Nested objects (stored as JSON string) --
    "ids":                  {"type": "json"},
    "summary_stats":        {"type": "json"},
    "international":        {"type": "json"},

    # -- Arrays (stored as JSON string) --
    "ancestors":            {"type": "json"},
    "related_concepts":     {"type": "json"},
    "counts_by_year":       {"type": "json"},
}
