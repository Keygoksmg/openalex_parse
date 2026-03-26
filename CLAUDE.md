# openalex_parse

## Project Overview
Generic pipeline for parsing raw OpenAlex gz JSON snapshots into parquet.

## Architecture
**Layer 1 (Base Extraction)**: `openalex_parse/parse.py` — streams gz JSON → parquet via DuckDB. User-defined schema controls which fields to extract. Memory-safe at any scale.

**Layer 2 (Derived Tables)**: `openalex_parse/derived/` — explode arrays, join tables, filter from base parquet. Ad hoc per project.

## Directory Structure
```
openalex_parse/
├── CLAUDE.md
├── openalex_parse/          # Python package
│   ├── parse.py             # Layer 1: gz JSON → parquet (DuckDB engine)
│   ├── schema_detect.py     # Schema detection + diff utility
│   ├── schemas/
│   │   └── works.py         # User-defined schema for works (39 fields)
│   └── derived/
│       └── work_topics.py   # Layer 2: paper × topic exploded table
├── tests/
│   └── test_parse_works.py  # 26 tests
├── claude_codes/            # Exploratory (gitignored)
│   ├── codes/
│   ├── notebooks/
│   ├── figures/
│   ├── plans/
│   └── findings/
└── data/                    # Data (gitignored)
    ├── intermediates/
    └── schemas/
```

## Key Design Decisions
- **User-defined schema** — user controls exactly which fields to extract via schema files
- **JSON string storage** — nested objects/arrays stored as JSON strings in parquet; exploding happens in Layer 2
- **DuckDB streaming engine** — memory-safe for any dataset size
- **Generic parser** — works for any entity type by pointing `--schema` at different config files
- **All CLI params required** — `--input`, `--output`, `--schema` must be explicit

## Key Commands
```bash
# Parse works
python -m openalex_parse.parse \
    --input <partition_dir> --output <output.parquet> --schema openalex_parse/schemas/works.py

# Schema detection
python -m openalex_parse.schema_detect --data-dir <entity_dir> --detect-only

# Derived table: work-topics
python -m openalex_parse.derived.work_topics --input <works.parquet> --output <work_topics.parquet>

# Run tests
python -m pytest tests/ -v
```

## Data Sources
- **Nov 2025 snapshot**: `/share/yin/openalex-2025_11_17/data/works/`
- **Mar 2026 snapshot**: `/share/yin/openalex-2026_03_26/`

## Dependencies
Python 3.12 (conda env: py312uv), polars, duckdb, pytest
