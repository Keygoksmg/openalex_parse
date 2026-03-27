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
- [DuckDB](https://duckdb.org/) (Layer 1: gz JSON → parquet)
- [Polars](https://pola.rs/) (Layer 2: derived tables + downstream queries)
- pytest (tests)

## High-Level Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│  Layer 1: Base Extraction — DuckDB (done once per snapshot)      │
│                                                                  │
│  Raw gz JSON  ──►  Parquet directory (one file per partition)     │
│  (580 GB)          works/*.parquet, authors/*.parquet, etc.      │
│                                                                  │
│  - DuckDB read_json(columns=...) — streams gz, memory safe      │
│  - User-defined schema controls which fields to extract          │
│  - parse.py (single file) or parse_partitioned.py (per-partition)│
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  Layer 2: Derived Tables — Polars (ad hoc, per project)          │
│                                                                  │
│  Parquet  ──►  Analysis-ready tables                             │
│                work_base, work_author_affiliations, etc.         │
│                                                                  │
│  - Polars scan_parquet → json_decode → explode → sink_parquet   │
│  - Fully streaming: constant memory, no temp files              │
│  - Each derived table is a single script                        │
└──────────────────────────────────────────────────────────────────┘
```

## Structure

```
openalex_parse/              # repo root
├── openalex_parse/          # Python package
│   ├── parse.py             # Layer 1: gz JSON → single parquet (DuckDB)
│   ├── parse_partitioned.py # Layer 1: gz JSON → partitioned parquet dir (DuckDB)
│   ├── schema_detect.py     # Detect fields, diff against schema, auto-generate
│   ├── schemas/             # User-defined schemas (21 entity types)
│   │   ├── works.py         # 65 fields (union across snapshots)
│   │   ├── authors.py       # 18 fields
│   │   ├── institutions.py  # 29 fields
│   │   ├── sources.py       # 36 fields
│   │   └── ...              # 17 more entity types
│   └── derived/             # Layer 2: all Polars streaming (scan → sink)
│       ├── work_base.py                # Paper-level flat table
│       ├── work_title_abstracts.py     # Reconstruct abstracts from inverted index
│       ├── work_author_affiliations.py # Paper × author × institution
│       ├── work_topics.py              # Paper × topic with hierarchy
│       ├── work_referenced_works.py    # Paper × cited paper
│       ├── work_counts_by_year.py      # Paper × year citation counts
│       ├── work_locations.py           # Paper × location/source
│       └── work_ids.py                 # Paper with all ID types
└── tests/
    ├── test_parse_works.py
    ├── test_schemas.py
    └── test_derived.py
```

## Quick Start

All commands run from the repo root. Replace `/path/to/openalex/` with wherever you downloaded the [OpenAlex snapshot](https://docs.openalex.org/download-all-data/download-to-your-machine).

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
# Auto-generate (samples earliest + latest partitions for robustness)
python -m openalex_parse.schema_detect \
    --data-dir /path/to/openalex/data/works \
    --generate openalex_parse/schemas/works.py
```

Or edit manually. Each schema file defines a dict named `*_SCHEMA` (e.g., `WORKS_SCHEMA`):

- `"str"`, `"int"`, `"float"`, `"bool"` → typed scalar columns
- `"json"` → stored as valid JSON string (for nested objects and arrays)

Fields in the schema but missing from the data become `NULL` automatically — safe to use one schema across snapshots.

### 3. Parse to parquet (Layer 1)

Two parsers available:

```bash
# parse.py — single output file (simpler, good for small entities)
python -m openalex_parse.parse \
    --input /path/to/openalex/data/authors \
    --output data/intermediates/authors.parquet \
    --schema openalex_parse/schemas/authors.py

# parse_partitioned.py — one output file per partition (recommended for large entities)
# Lower memory, progress tracking, resume support
python -m openalex_parse.parse_partitioned \
    --input /path/to/openalex/data/works \
    --output data/intermediates/works \
    --schema openalex_parse/schemas/works.py

# Test with limit
python -m openalex_parse.parse \
    --input /path/to/openalex/data/works/updated_date=2025-11-11 \
    --output data/intermediates/works_test.parquet \
    --schema openalex_parse/schemas/works.py \
    --limit 5000
```

### 4. Query the output

```python
import polars as pl

# Single file
df = pl.scan_parquet("data/intermediates/authors.parquet")

# Partitioned directory (works as one table)
df = pl.scan_parquet("data/intermediates/works/*.parquet")

df.filter(pl.col("publication_year") == 2024).head(10).collect()
```

### 5. Create derived tables (Layer 2)

All derived scripts use Polars streaming (`scan_parquet` → `sink_parquet`), keeping memory constant regardless of data size. Input can be a single file or a glob.

```bash
# Paper-level flat table (scalar fields + unnested primary_topic, open_access, source)
python -m openalex_parse.derived.work_base \
    --input data/intermediates/works/*.parquet \
    --output data/intermediates/work_base.parquet

# Reconstruct abstracts from inverted index
python -m openalex_parse.derived.work_title_abstracts \
    --input data/intermediates/works/*.parquet \
    --output data/intermediates/work_title_abstracts.parquet

# Paper × author × institution (all affiliations, not just first)
python -m openalex_parse.derived.work_author_affiliations \
    --input data/intermediates/works/*.parquet \
    --output data/intermediates/work_author_affiliations.parquet

# Paper × topic with hierarchy
python -m openalex_parse.derived.work_topics \
    --input data/intermediates/works/*.parquet \
    --output data/intermediates/work_topics.parquet

# Paper × cited paper
python -m openalex_parse.derived.work_referenced_works \
    --input data/intermediates/works/*.parquet \
    --output data/intermediates/work_referenced_works.parquet

# Paper × year citation counts
python -m openalex_parse.derived.work_counts_by_year \
    --input data/intermediates/works/*.parquet \
    --output data/intermediates/work_counts_by_year.parquet

# Paper × location/source
python -m openalex_parse.derived.work_locations \
    --input data/intermediates/works/*.parquet \
    --output data/intermediates/work_locations.parquet

# Paper with all ID types (openalex, doi, mag, pmid, pmcid)
python -m openalex_parse.derived.work_ids \
    --input data/intermediates/works/*.parquet \
    --output data/intermediates/work_ids.parquet
```

## Resource Requirements

**Layer 1 (DuckDB)** — gz decompression + JSON parsing is CPU-bound:

| Task | CPU | RAM | Disk | Time |
|------|-----|-----|------|------|
| Full works parse_partitioned (Mar 2026, 580 GB gz, 482M rows) | 8 cores | 120 GB | 873 GB output | 7h 5m |
| Full works parse single-file (same data) | 8 cores | 128 GB | 873 GB output | 6h 44m |
| Single partition (~9 GB gz) | 2 cores | 8 GB | ~15 GB output | ~10 min |
| Small test (5K records) | 1 core | 2 GB | ~10 MB output | ~4 sec |

**Layer 2 (Polars streaming)** — constant memory via `sink_parquet`:

| Task | CPU | RAM | Time |
|------|-----|-----|------|
| Derived tables (work_base, topics, etc.) | 4 cores | 32 GB | minutes |

**Minimum**: 1 CPU and 4 GB RAM will work for any task, just slower.

## Adding new entity types

1. Auto-generate schema: `python -m openalex_parse.schema_detect --data-dir .../data/sources --generate openalex_parse/schemas/sources.py`
2. Review and edit the generated schema if needed
3. Parse: `python -m openalex_parse.parse_partitioned --input .../data/sources --output .../sources --schema openalex_parse/schemas/sources.py`

No new parser code needed — the same `parse.py` / `parse_partitioned.py` handles everything.
Schemas already exist for all 21 entity types in the Mar 2026 snapshot.

## Tests

```bash
python -m pytest tests/ -v
```
