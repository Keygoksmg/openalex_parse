# openalex_parse

Generic pipeline for parsing raw [OpenAlex](https://openalex.org/) gz JSON snapshots into parquet.
Works with any entity type (works, authors, concepts, etc.) — just point it
at the right schema config.

## Why

OpenAlex distributes data as gzipped JSON files partitioned by date (~1TB for works alone).
Querying raw JSON is slow and memory-intensive. This tool converts it into columnar parquet
so downstream analysis can read only the columns and rows it needs — fast and cheap.

## Requirements

- Python 3.12+
- [DuckDB](https://duckdb.org/) (streaming engine)
- [Polars](https://pola.rs/) (downstream queries)
- pytest (tests)

## High-Level Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│  Layer 1: Base Extraction (done once per snapshot)              │
│                                                                 │
│  Raw gz JSON  ──►  Parquet (one file per entity type)           │
│  (965 GB)          works.parquet, authors.parquet, etc.         │
│                                                                 │
│  - Simple field extraction, no exploding or joins               │
│  - User-defined schema controls which fields to extract         │
│  - DuckDB engine: streams data, memory safe at any scale        │
│  - Expensive step (reads all raw data), but only done once      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Layer 2: Derived Tables (ad hoc, per project)                  │
│                                                                 │
│  Parquet  ──►  Exploded/joined tables                           │
│                work_author_affiliations, work_topics, etc.      │
│                                                                 │
│  - Complex operations: explode arrays, join tables, filter      │
│  - Reads only needed columns from parquet (fast)                │
│  - Use DuckDB or Polars — whichever fits the task               │
│  - Cheap to re-run, iterate, and experiment                     │
└─────────────────────────────────────────────────────────────────┘
```

## Structure

```
openalex_parse/              # repo root
├── openalex_parse/          # Python package
│   ├── parse.py             # Generic parser: gz JSON → parquet (DuckDB engine)
│   ├── schema_detect.py     # Detect fields in raw data, diff against user schema
│   ├── schemas/
│   │   └── works.py         # User-defined schema for works (add authors.py, etc.)
│   └── derived/
│       └── work_topics.py   # Example Layer 2: paper × topic exploded table
└── tests/
    └── test_parse_works.py
```

## Quick Start

All commands run from the repo root. Replace `/path/to/openalex/` with wherever you downloaded the [OpenAlex snapshot](https://docs.openalex.org/download-all-data/download-to-your-machine) (e.g. via `aws s3 sync`).

### 1. Detect schema (what fields exist in the raw data?)

```bash
# Detect and diff against user schema
python -m openalex_parse.schema_detect \
    --data-dir /path/to/openalex/data/works \
    --schema openalex_parse/schemas/works.py

# Just detect, no comparison
python -m openalex_parse.schema_detect \
    --data-dir /path/to/openalex/data/authors \
    --detect-only
```

### 2. Edit schema config

Edit `openalex_parse/schemas/works.py` to control which fields are extracted:

- `"str"`, `"int"`, `"float"`, `"bool"` → typed scalar columns
- `"json"` → stored as valid JSON string (for nested objects and arrays)

Each schema file must define a dict named `*_SCHEMA` (e.g., `WORKS_SCHEMA`).

### 3. Parse to parquet (Layer 1)

All three params (`--input`, `--output`, `--schema`) are required.

```bash
# Works — test on one partition
python -m openalex_parse.parse \
    --input /path/to/openalex/data/works/updated_date=2025-11-11 \
    --output data/intermediates/works_test.parquet \
    --schema openalex_parse/schemas/works.py \
    --limit 5000

# Works — full directory
python -m openalex_parse.parse \
    --input /path/to/openalex/data/works \
    --output data/intermediates/works.parquet \
    --schema openalex_parse/schemas/works.py

# Authors
python -m openalex_parse.parse \
    --input /path/to/openalex/data/authors \
    --output data/intermediates/authors.parquet \
    --schema openalex_parse/schemas/authors.py
```

### 4. Query the output

```python
import polars as pl

df = pl.scan_parquet("data/intermediates/works.parquet")
df.filter(pl.col("publication_year") == 2024).head(10).collect()
```

### 5. Create derived tables (Layer 2)

Arrays (authorships, topics, etc.) are stored as JSON strings.
Explode them using DuckDB or Polars:

```python
# DuckDB — SQL style
import duckdb

duckdb.sql("""
    SELECT
        id,
        publication_year,
        unnest(from_json(authorships, '["json"]')) AS auth
    FROM read_parquet('data/intermediates/works.parquet')
    WHERE publication_year = 2024
""")

# Polars — Python style
import json, polars as pl

df = pl.read_parquet("data/intermediates/works.parquet",
                     columns=["id", "publication_year", "authorships"])
rows = []
for row in df.iter_rows(named=True):
    for auth in json.loads(row["authorships"] or "[]"):
        inst = (auth.get("institutions") or [{}])[0] if auth.get("institutions") else {}
        rows.append({
            "id": row["id"],
            "author_id": auth.get("author", {}).get("id"),
            "author_display_name": auth.get("author", {}).get("display_name"),
            "institution_id": inst.get("id"),
            "institution_country_code": inst.get("country_code"),
        })
work_authors = pl.DataFrame(rows)
```

## Adding new entity types

To parse `authors/`, `sources/`, etc.:

1. Detect: `python -m openalex_parse.schema_detect --data-dir .../data/authors --detect-only`
2. Create `openalex_parse/schemas/authors.py` with an `AUTHORS_SCHEMA` dict
3. Parse: `python -m openalex_parse.parse --input .../data/authors --output .../authors.parquet --schema openalex_parse/schemas/authors.py`

No new parser code needed — the same `parse.py` handles everything.

## Tests

```bash
python -m pytest tests/test_parse_works.py -v
```
