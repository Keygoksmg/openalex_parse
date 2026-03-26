"""
User-defined schema config for the awards table.

Derived from schema detection on the Mar 2026 snapshot.

Each entry: field_name -> {"type": output_type}
  - "str", "int", "float", "bool" -> typed scalar columns
  - "json" -> stored as JSON string (nested objects and arrays)
"""

AWARDS_SCHEMA = {
    # ── Scalar fields ────────────────────────────────────────────────────────
    "id":                           {"type": "str"},
    "display_name":                 {"type": "str"},
    "doi":                          {"type": "str"},
    "description":                  {"type": "str"},
    "funder_award_id":              {"type": "str"},
    "funder_scheme":                {"type": "str"},
    "funding_type":                 {"type": "str"},
    "currency":                     {"type": "str"},
    "amount":                       {"type": "float"},
    "start_date":                   {"type": "str"},
    "start_year":                   {"type": "int"},
    "end_date":                     {"type": "str"},
    "end_year":                     {"type": "int"},
    "funded_outputs_count":         {"type": "int"},
    "landing_page_url":             {"type": "str"},
    "provenance":                   {"type": "str"},
    "created_date":                 {"type": "str"},
    "updated_date":                 {"type": "str"},
    "works_api_url":                {"type": "str"},

    # ── Nested objects (stored as JSON string) ───────────────────────────────
    "funder":                       {"type": "json"},
    "lead_investigator":            {"type": "json"},
    "co_lead_investigator":         {"type": "json"},

    # ── Arrays (stored as JSON string) ───────────────────────────────────────
    "funded_outputs":               {"type": "json"},
    "investigators":                {"type": "json"},
}
