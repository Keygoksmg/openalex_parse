"""
User-defined schema config for the works table.

Union of all fields across snapshots:
  - Nov 2025 standard (data/works)
  - Nov 2025 legacy (legacy-data/works)
  - Mar 2026 (data/works)

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)

Fields that only exist in one snapshot will be null in the other.
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
    "language_id":                  {"type": "str"},
    "type":                         {"type": "str"},
    "type_crossref":                {"type": "str"},
    "type_id":                      {"type": "str"},
    "cited_by_count":               {"type": "int"},
    "fwci":                         {"type": "float"},
    "referenced_works_count":       {"type": "int"},
    "authors_count":                {"type": "int"},
    "concepts_count":               {"type": "int"},
    "topics_count":                 {"type": "int"},
    "locations_count":              {"type": "int"},
    "institutions_distinct_count":  {"type": "int"},
    "countries_distinct_count":     {"type": "int"},
    "has_abstract":                 {"type": "bool"},
    "has_fulltext":                 {"type": "bool"},
    "is_retracted":                 {"type": "bool"},
    "is_paratext":                  {"type": "bool"},
    "is_xpac":                      {"type": "bool"},
    "publisher":                    {"type": "str"},
    "doi_registration_agency":      {"type": "str"},
    "fulltext_origin":              {"type": "str"},
    "cited_by_api_url":             {"type": "str"},
    "created_date":                 {"type": "str"},
    "updated":                      {"type": "str"},
    "updated_date":                 {"type": "str"},

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
    "summary_stats":                    {"type": "json"},
    "has_content":                      {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "indexed_in":                       {"type": "json"},
    "authorships":                      {"type": "json"},
    "keywords":                         {"type": "json"},
    "concepts":                         {"type": "json"},
    "topics":                           {"type": "json"},
    "related_works":                    {"type": "json"},
    "referenced_works":                 {"type": "json"},
    "locations":                        {"type": "json"},
    "counts_by_year":                   {"type": "json"},
    "corresponding_author_ids":         {"type": "json"},
    "corresponding_institution_ids":    {"type": "json"},
    "sustainable_development_goals":    {"type": "json"},
    "grants":                           {"type": "json"},
    "mesh":                             {"type": "json"},
    "datasets":                         {"type": "json"},
    "versions":                         {"type": "json"},
    "institution_assertions":           {"type": "json"},
    "awards":                           {"type": "json"},
    "funders":                          {"type": "json"},
    "institutions":                     {"type": "json"},

    # ── Special ──────────────────────────────────────────────────────────────
    "abstract_inverted_index":      {"type": "json"},
}
