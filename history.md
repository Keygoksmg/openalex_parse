# OpenAlex Parse — Session History (2026-03-26)

## What We Built

A generic pipeline for parsing raw OpenAlex gz JSON snapshots into parquet, organized as a Python package.

## Two-Layer Architecture

**Layer 1: Base Extraction** (gz JSON → parquet, done once per snapshot)
- Simple field extraction, no exploding or joins
- User-defined schema controls which fields to extract
- DuckDB engine: streams data, memory safe at any scale
- Output: one parquet file per entity type (works.parquet, authors.parquet, etc.)

**Layer 2: Derived Tables** (parquet → exploded/joined tables, ad hoc per project)
- Complex operations: explode arrays, join tables, filter
- Reads only needed columns from base parquet (fast, cheap)
- Use DuckDB or Polars

## Current Directory Structure

```
/share/yin/kk929_codes/openalex_parse/     ← git repo root (needs git init)
├── .gitignore                             ← needs creating
├── README.md                              ← in openalex_parse/ subfolder, should move to root
├── CLAUDE.md
├── openalex_parse/                        ← Python package
│   ├── __init__.py
│   ├── parse.py                           ← Layer 1: gz JSON → parquet (DuckDB engine)
│   ├── schema_detect.py                   ← Schema detection + diff utility
│   ├── README.md                          ← Package docs (move to repo root)
│   ├── schemas/
│   │   ├── __init__.py
│   │   └── works.py                       ← User-defined schema for works
│   └── derived/
│       ├── __init__.py
│       └── work_topics.py                 ← Layer 2: paper × topic exploded table
├── tests/
│   ├── __init__.py
│   └── test_parse_works.py                ← 26 tests, all passing
├── claude_codes/                          ← Exploratory (gitignore)
│   ├── codes/
│   ├── notebooks/
│   ├── figures/
│   ├── plans/
│   │   ├── plan.md                        ← Full implementation plan
│   │   └── 20260326_download_openalex.md  ← S3 download notes
│   └── findings/
└── data/                                  ← Data (gitignore)
    ├── intermediates/
    │   ├── works_test.parquet             ← Old test output (can delete)
    │   ├── works_test_duckdb.parquet      ← 5000 records, DuckDB output (working)
    │   └── work_topics.parquet            ← Exploded topics test
    └── schemas/                           ← Empty (schemas moved to package)
```

## Remaining Setup

1. **Git init** the repo at `/share/yin/kk929_codes/openalex_parse/`
2. **Create .gitignore**:
   ```
   data/
   claude_codes/
   __pycache__/
   *.pyc
   .pytest_cache/
   logs/
   *.out
   *.err
   ```
3. **Move README.md** from `openalex_parse/README.md` to repo root
4. The `mv` from `openalex_manage` → `openalex_parse` caused the sandbox to lose its working directory — need to start a new session from `/share/yin/kk929_codes/openalex_parse/`

## Key Commands

```bash
# Schema detection (auto-picks latest partition)
python -m openalex_parse.schema_detect

# Schema detection for other entities
python -m openalex_parse.schema_detect \
    --data-dir /share/yin/openalex-2025_11_17/data/authors \
    --detect-only

# Parse works (all three params required)
python -m openalex_parse.parse \
    --input /share/yin/openalex-2025_11_17/data/works/updated_date=2025-11-11 \
    --output data/intermediates/works_test.parquet \
    --schema openalex_parse/schemas/works.py \
    --limit 5000

# Derived table: work-topics
python -m openalex_parse.derived.work_topics \
    --input data/intermediates/works_test_duckdb.parquet \
    --output data/intermediates/work_topics.parquet

# Run tests
python -m pytest tests/test_parse_works.py -v
```

## Key Design Decisions

1. **User-defined schema** — user controls exactly which fields to extract via `openalex_parse/schemas/works.py`. No auto-extraction; schema detection is a separate utility that shows what's available and what's missing.

2. **JSON string storage** — nested objects and arrays stored as valid JSON strings in parquet. This keeps the base table simple and generic. Exploding happens in Layer 2.

3. **DuckDB engine** — replaced initial Python/Polars parser (which loaded everything into memory) with DuckDB streaming. Memory safe for any dataset size.

4. **Generic parser** — `parse.py` works for any entity type (works, authors, etc.) by pointing `--schema` at different config files. No entity-specific code.

5. **No hive partitioning** — removed for simplicity. Single parquet file per entity. Polars/DuckDB row-group pruning is fast enough for queries.

6. **All params required** — `--input`, `--output`, `--schema` must all be specified explicitly. No hidden defaults that could cause accidental runs on wrong data.

## Data Sources

- **Nov 2025 snapshot**: `/share/yin/openalex-2025_11_17/data/works/` (965GB, 179 partitions, used for prototyping)
- **2026-03-26 snapshot**: `/share/yin/openalex-2026_03_26/` (downloading via `aws s3 sync`, works/ not yet available)
- **Test partition**: `updated_date=2025-11-11` (218K records, 267MB, 1 gz file)

## Schema: works.py (39 fields)

Scalar: id, doi, title, display_name, publication_year, publication_date, language, type, cited_by_count, fwci, referenced_works_count, authors_count, institutions_distinct_count, countries_distinct_count, has_abstract, is_retracted, is_paratext, publisher, created_date

JSON (nested objects): ids, primary_location, open_access, best_oa_location, citation_normalized_percentile, cited_by_percentile_year, apc_list, apc_paid, biblio, primary_topic

JSON (arrays): indexed_in, authorships, keywords, concepts, topics, related_works, referenced_works, locations, counts_by_year

JSON (special): abstract_inverted_index

15 fields exist in the data but were intentionally excluded from schema (awards, cited_by_api_url, corresponding_author_ids, corresponding_institution_ids, fulltext, funders, grants, has_content, indexed_in_crossref, institutions, is_xpac, locations_count, mesh, sustainable_development_goals, topics_key). User can add them to works.py if needed.

## Dependencies

- Python 3.12 (conda env: py312uv)
- polars
- duckdb (v1.5.1)
- pytest

## BigQuery Reference

The schema was derived from user's existing BigQuery SQL at:
https://github.com/Keygoksmg/Notes/tree/main/BigQuery/OpenAlex

Files: `works_to_csv.sql`, `work_author_institutions_flat.sql`, `work_abstract.sql`, `works_RaoStirlingDiversity.sql`
