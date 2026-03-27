# openalex_parse

## Project Overview
Generic pipeline for parsing raw OpenAlex gz JSON snapshots into parquet.

## Architecture
**Layer 1 (Base Extraction — DuckDB)**: `parse.py` / `parse_partitioned.py` — streams gz JSON → parquet via DuckDB `read_json(columns=...)`. User-defined schema controls extraction. Missing columns become NULL. Memory-safe.

**Layer 2 (Derived Tables — Polars)**: `derived/` — all scripts use Polars `scan_parquet` → `json_decode` → `explode` → `sink_parquet` for streaming, constant-memory processing. No DuckDB, no temp files.

## Directory Structure
```
openalex_parse/
├── CLAUDE.md
├── openalex_parse/          # Python package
│   ├── parse.py             # Layer 1: gz JSON → single parquet (DuckDB)
│   ├── parse_partitioned.py # Layer 1: gz JSON → partitioned parquet dir (DuckDB)
│   ├── schema_detect.py     # Schema detection, diff, and auto-generation
│   ├── schemas/             # User-defined schemas (21 entity types)
│   │   ├── works.py         # 65 fields (union across Nov 2025 + Mar 2026)
│   │   ├── authors.py       # 18 fields
│   │   ├── institutions.py  # 29 fields
│   │   ├── sources.py       # 36 fields
│   │   └── ...              # 17 more entity types
│   └── derived/             # Layer 2: all Polars streaming
│       ├── work_base.py                # Paper-level flat table
│       ├── work_title_abstracts.py     # Reconstruct abstracts from inverted index
│       ├── work_author_affiliations.py # Paper × author × institution
│       ├── work_topics.py              # Paper × topic with hierarchy
│       ├── work_referenced_works.py    # Paper × cited paper
│       ├── work_counts_by_year.py      # Paper × year citation counts
│       ├── work_locations.py           # Paper × location/source
│       └── work_ids.py                 # Paper with all ID types
├── tests/
│   ├── test_parse_works.py  # Parser + works round-trip tests
│   ├── test_schemas.py      # Schema loading + round-trip for all entities
│   └── test_derived.py      # Derived table tests (abstract, authorships)
├── claude_codes/            # Exploratory (gitignored)
└── data/                    # Data (gitignored)
```

## Key Design Decisions
- **DuckDB for Layer 1** — best at gz decompression + JSON parsing (C++ engine)
- **Polars for Layer 2** — `scan_parquet` → `sink_parquet` streaming keeps memory constant
- **Explicit column spec** — `read_json(columns=...)` so missing columns become NULL
- **Cross-snapshot schemas** — one schema covers multiple snapshots (union of all fields)
- **JSON string storage** — nested objects/arrays stored as JSON strings; exploding in Layer 2
- **Partitioned output** — `parse_partitioned.py` writes one parquet per partition for lower memory
- **Schema auto-generation** — `schema_detect --generate` samples earliest + latest partitions

## Key Commands
```bash
# Parse works (partitioned — recommended for large entities)
python -m openalex_parse.parse_partitioned \
    --input <data_dir> --output <output_dir> --schema openalex_parse/schemas/works.py

# Parse works (single file — simpler for small entities)
python -m openalex_parse.parse \
    --input <data_dir> --output <output.parquet> --schema openalex_parse/schemas/works.py

# Schema detection + auto-generate
python -m openalex_parse.schema_detect --data-dir <entity_dir> --generate openalex_parse/schemas/<entity>.py

# Derived tables (all use Polars streaming, input can be file or glob)
python -m openalex_parse.derived.work_base --input <works/*.parquet> --output <work_base.parquet>
python -m openalex_parse.derived.work_title_abstracts --input <works/*.parquet> --output <output.parquet>
python -m openalex_parse.derived.work_author_affiliations --input <works/*.parquet> --output <output.parquet>
python -m openalex_parse.derived.work_topics --input <works/*.parquet> --output <output.parquet>
python -m openalex_parse.derived.work_referenced_works --input <works/*.parquet> --output <output.parquet>
python -m openalex_parse.derived.work_counts_by_year --input <works/*.parquet> --output <output.parquet>
python -m openalex_parse.derived.work_locations --input <works/*.parquet> --output <output.parquet>
python -m openalex_parse.derived.work_ids --input <works/*.parquet> --output <output.parquet>

# Run tests
python -m pytest tests/ -v
```

## Data Sources
- **Nov 2025 snapshot**: `/share/yin/openalex-2025_11_17/data/`
- **Nov 2025 legacy**: `/share/yin/openalex-2025_11_17/legacy-data/`
- **Mar 2026 snapshot**: `/share/yin/openalex-2026_03_26/data/`
- **Mar 2026 parsed**: `/share/yin/openalex-2026_03_26/data-parsed/`

## Resource Requirements
- **Layer 1 (DuckDB)**: CPU-bound (gz decompression + JSON parsing)
  - Full works `parse_partitioned` (580 GB gz, 482M rows → 873 GB): 8 CPUs, 120 GB RAM, 7h 5m
  - Single partition (~9 GB gz): 2 CPUs, 8 GB RAM, ~10 min
- **Layer 2 (Polars streaming)**: constant memory via `sink_parquet`
  - Derived tables: 4 CPUs, 32 GB RAM, minutes each
- **Minimum** (will work but slower): 1 CPU, 4 GB RAM

## Dependencies
Python 3.12 (conda env: py312uv), polars, duckdb, pytest
