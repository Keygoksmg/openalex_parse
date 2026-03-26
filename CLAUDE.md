# openalex_parse

## Project Overview
Generic pipeline for parsing raw OpenAlex gz JSON snapshots into parquet.

## Architecture
**Layer 1 (Base Extraction)**: `openalex_parse/parse.py` — streams gz JSON → parquet via DuckDB with explicit `read_json(columns=...)`. User-defined schema controls which fields to extract. Missing columns become NULL automatically. Memory-safe at any scale.

**Layer 2 (Derived Tables)**: `openalex_parse/derived/` — explode arrays, join tables, filter from base parquet. Ad hoc per project.

## Directory Structure
```
openalex_parse/
├── CLAUDE.md
├── openalex_parse/          # Python package
│   ├── parse.py             # Layer 1: gz JSON → parquet (DuckDB engine)
│   ├── schema_detect.py     # Schema detection, diff, and auto-generation
│   ├── schemas/             # User-defined schemas (21 entity types)
│   │   ├── works.py         # 65 fields (union across Nov 2025 + Mar 2026)
│   │   ├── authors.py       # 18 fields
│   │   ├── institutions.py  # 29 fields
│   │   ├── sources.py       # 36 fields
│   │   └── ...              # 17 more: awards, concepts, continents, countries,
│   │                        # domains, fields, funders, institution_types,
│   │                        # keywords, languages, licenses, publishers, sdgs,
│   │                        # source_types, subfields, topics, work_types
│   └── derived/
│       ├── work_topics.py              # Paper × topic exploded table
│       ├── work_title_abstracts.py     # Reconstruct abstracts from inverted index
│       └── work_author_institutions.py # Paper × author × institution flat table
├── tests/
│   ├── test_parse_works.py  # Parser + works round-trip tests
│   ├── test_schemas.py      # Schema loading + round-trip for authors/concepts/institutions
│   └── test_derived.py      # Derived table tests (abstract, authorships)
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
- **Explicit column spec** — `parse.py` uses `read_json(columns=...)` instead of `read_json_auto`, so missing columns become NULL and no separate column detection step is needed
- **Cross-snapshot schemas** — one schema covers multiple snapshots (union of all fields); missing fields in a snapshot are NULL
- **JSON string storage** — nested objects/arrays stored as JSON strings in parquet; exploding happens in Layer 2
- **DuckDB streaming engine** — memory-safe for any dataset size
- **Generic parser** — works for any entity type by pointing `--schema` at different config files
- **All CLI params required** — `--input`, `--output`, `--schema` must be explicit
- **Schema auto-generation** — `schema_detect --generate` samples earliest + latest partitions for robust field detection

## Key Commands
```bash
# Parse works
python -m openalex_parse.parse \
    --input <data_dir> --output <output.parquet> --schema openalex_parse/schemas/works.py

# Schema detection + diff
python -m openalex_parse.schema_detect --data-dir <entity_dir> --schema openalex_parse/schemas/works.py

# Auto-generate schema
python -m openalex_parse.schema_detect --data-dir <entity_dir> --generate openalex_parse/schemas/<entity>.py

# Derived tables
python -m openalex_parse.derived.work_topics --input <works.parquet> --output <work_topics.parquet>
python -m openalex_parse.derived.work_title_abstracts --input <works.parquet> --output <output.parquet>
python -m openalex_parse.derived.work_author_institutions --input <works.parquet> --output <output.parquet>

# Run tests
python -m pytest tests/ -v
```

## Data Sources
- **Nov 2025 snapshot**: `/share/yin/openalex-2025_11_17/data/`
- **Nov 2025 legacy**: `/share/yin/openalex-2025_11_17/legacy-data/`
- **Mar 2026 snapshot**: `/share/yin/openalex-2026_03_26/data/`
- **Mar 2026 parsed**: `/share/yin/openalex-2026_03_26/data-parsed/`

## Dependencies
Python 3.12 (conda env: py312uv), polars, duckdb, pytest
