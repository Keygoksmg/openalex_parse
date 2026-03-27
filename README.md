# openalex_parse

Generic pipeline for parsing raw [OpenAlex](https://openalex.org/) gz JSON snapshots into parquet.
Works with any entity type (works, authors, institutions, etc.) — just point it
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
│                work_author_institutions, work_topics, etc.      │
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
│   ├── schema_detect.py     # Detect fields, diff against schema, auto-generate schema files
│   ├── schemas/             # User-defined schemas (21 entity types)
│   │   ├── works.py         # 65 fields (union across snapshots)
│   │   ├── authors.py       # 18 fields
│   │   ├── institutions.py  # 29 fields
│   │   ├── sources.py       # 36 fields
│   │   └── ...              # awards, concepts, countries, domains, fields, funders,
│   │                        # institution_types, keywords, languages, licenses,
│   │                        # publishers, sdgs, source_types, subfields, topics, etc.
│   └── derived/
│       ├── work_topics.py              # Paper × topic exploded table
│       ├── work_title_abstracts.py     # Reconstruct abstracts from inverted index
│       └── work_author_institutions.py # Paper × author × institution flat table
└── tests/
    ├── test_parse_works.py  # Parser tests (works round-trip, integration)
    ├── test_schemas.py      # Schema loading + round-trip for all entities
    └── test_derived.py      # Derived table tests (abstract reconstruction, authorship explode)
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

### 2. Auto-generate or edit schema

```bash
# Auto-generate a schema file (samples earliest + latest partitions for robustness)
python -m openalex_parse.schema_detect \
    --data-dir /path/to/openalex/data/works \
    --generate openalex_parse/schemas/works.py
```

Or edit manually. Each schema file defines a dict named `*_SCHEMA` (e.g., `WORKS_SCHEMA`):

- `"str"`, `"int"`, `"float"`, `"bool"` → typed scalar columns
- `"json"` → stored as valid JSON string (for nested objects and arrays)

Fields in the schema but missing from the data become `NULL` automatically — safe to use one schema across snapshots.

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

Once Layer 1 converts raw .gz files into a queryable base parquet, you can create smaller, analysis-ready derived tables by unnesting and exploding the JSON array columns (authorships, topics, etc.).

#### CLI-based derived tables

```bash
# Work-topics
python -m openalex_parse.derived.work_topics \
    --input data/intermediates/works.parquet \
    --output data/intermediates/work_topics.parquet

# Work-title-abstracts (reconstructs abstract text from inverted index)
python -m openalex_parse.derived.work_title_abstracts \
    --input data/intermediates/works.parquet \
    --output data/intermediates/work_title_abstracts.parquet

# Work-author-institutions (one row per paper × author)
python -m openalex_parse.derived.work_author_institutions \
    --input data/intermediates/works.parquet \
    --output data/intermediates/work_author_institutions.parquet
```

#### Custom derived tables (DuckDB example)

```python
import duckdb

duckdb.sql("""
    COPY (
        SELECT
            w.id AS work_id,
            w.publication_year,
            a.author.id AS author_id,
            a.author.display_name AS author_name,
            a.institutions[1].id AS institution_id,
            a.institutions[1].country_code AS country_code,
            a.author_position
        FROM read_parquet('data/intermediates/works.parquet') w,
             unnest(from_json(w.authorships, '[{
                "author": {"id": "VARCHAR", "display_name": "VARCHAR"},
                "institutions": [{"id": "VARCHAR", "country_code": "VARCHAR"}],
                "author_position": "VARCHAR"
             }]')) AS a
    ) TO 'data/intermediates/work_authors.parquet' (FORMAT PARQUET);
""")
```

#### Custom derived tables (Polars example)

```python
import json
import polars as pl

df = pl.read_parquet(
    "data/intermediates/works.parquet",
    columns=["id", "publication_year", "authorships"],
)
rows = []
for row in df.iter_rows(named=True):
    for auth in json.loads(row["authorships"] or "[]"):
        inst = (auth.get("institutions") or [{}])[0] if auth.get("institutions") else {}
        rows.append({
            "work_id": row["id"],
            "publication_year": row["publication_year"],
            "author_id": auth.get("author", {}).get("id"),
            "author_name": auth.get("author", {}).get("display_name"),
            "institution_id": inst.get("id"),
            "country_code": inst.get("country_code"),
        })
work_authors = pl.DataFrame(rows)
work_authors.write_parquet("data/intermediates/work_authors.parquet")
```

## Resource Requirements

The parser uses DuckDB's streaming engine — it never loads the full dataset into memory.

| Task | CPU | RAM | Disk | Time |
|------|-----|-----|------|------|
| Parse full works (Mar 2026, 580 GB gz, 482M rows) | 8 cores | 128 GB | 873 GB output | 6h 44m |
| Parse single partition (~9 GB gz) | 2 cores | 8 GB | ~15 GB output | ~10 min |
| Parse small test (5K records) | 1 core | 2 GB | ~10 MB output | ~4 sec |
| Derived tables from full parquet | 1-2 cores | 4-8 GB | varies | minutes |
| Schema detection | 1 core | 1 GB | none | seconds |

**Minimum**: 1 CPU and 4 GB RAM will work for any task, just slower.

**CPU**: More cores help — DuckDB parallelizes gz decompression and JSON parsing across threads. 8 cores is a good balance for full parses.

**Memory**: DuckDB streams data, but peak RSS reached 108 GB for the full works parse (482M rows, 65 columns). Allocate 128 GB for full parses; 8 GB is enough for single partitions or derived tables.

**Disk**: Output parquet is roughly 1.5x the size of the compressed gz input (JSON string columns compress less than native columnar). For the full Mar 2026 works: 580 GB input → 873 GB output.

## Adding new entity types

To parse `sources/`, `funders/`, etc.:

1. Auto-generate schema: `python -m openalex_parse.schema_detect --data-dir .../data/sources --generate openalex_parse/schemas/sources.py`
2. Review and edit the generated schema if needed
3. Parse: `python -m openalex_parse.parse --input .../data/sources --output .../sources.parquet --schema openalex_parse/schemas/sources.py`

No new parser code needed — the same `parse.py` handles everything.
Schemas already exist for all 21 entity types in the Mar 2026 snapshot.

## Tests

```bash
python -m pytest tests/ -v
```
