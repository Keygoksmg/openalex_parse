"""
User-defined schema config for the works table.

Derived from BigQuery works_to_csv.sql, adjusted after schema detection on
the Nov 2025 snapshot (updated_date=2025-11-11).

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)
"""

WORKS_SCHEMA = {
    # ── Scalar fields ────────────────────────────────────────────────────────
    "id":                           {"type": "str"},
    "doi":                          {"type": "str"},
    "title":                        {"type": "str"},
    "display_name":                 {"type": "str"},
    "publication_year":             {"type": "int"},
    "publication_date":             {"type": "str"},
    "language":                     {"type": "str"},
    "type":                         {"type": "str"},
    "cited_by_count":               {"type": "int"},
    "fwci":                         {"type": "float"},
    "referenced_works_count":       {"type": "int"},
    "authors_count":                {"type": "int"},
    "institutions_distinct_count":  {"type": "int"},
    "countries_distinct_count":     {"type": "int"},
    "has_abstract":                 {"type": "bool"},
    "is_retracted":                 {"type": "bool"},
    "is_paratext":                  {"type": "bool"},
    "publisher":                    {"type": "str"},
    "created_date":                 {"type": "str"},

    # ── Nested objects (stored as JSON string) ───────────────────────────────
    "ids":                              {"type": "json"},
    "primary_location":                 {"type": "json"},
    "open_access":                      {"type": "json"},
    "best_oa_location":                 {"type": "json"},
    "citation_normalized_percentile":   {"type": "json"},
    "cited_by_percentile_year":         {"type": "json"},
    "apc_list":                         {"type": "json"},
    "apc_paid":                         {"type": "json"},
    "biblio":                           {"type": "json"},
    "primary_topic":                    {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "indexed_in":                   {"type": "json"},
    "authorships":                  {"type": "json"},
    "keywords":                     {"type": "json"},
    "concepts":                     {"type": "json"},
    "topics":                       {"type": "json"},
    "related_works":                {"type": "json"},
    "referenced_works":             {"type": "json"},
    "locations":                    {"type": "json"},
    "counts_by_year":               {"type": "json"},

    # ── Special ──────────────────────────────────────────────────────────────
    "abstract_inverted_index":      {"type": "json"},
}