"""
Generic parser for OpenAlex gz JSON into parquet using DuckDB.

Streams gz JSON files through DuckDB — memory safe for any dataset size.
Works with any entity type (works, authors, concepts, etc.) by pointing
to the appropriate schema config file.

Usage:
    # Works — full directory
    python -m openalex_parse.parse \
        --input /path/to/openalex/data/works \
        --output data/intermediates/works.parquet \
        --schema openalex_parse/schemas/works.py

    # Works — test on a single partition with limit
    python -m openalex_parse.parse \
        --input /path/to/openalex/data/works/updated_date=2025-11-11 \
        --output data/intermediates/works_test.parquet \
        --schema openalex_parse/schemas/works.py \
        --limit 5000

    # Authors
    python -m openalex_parse.parse \
        --input /path/to/openalex/data/authors \
        --output data/intermediates/authors.parquet \
        --schema openalex_parse/schemas/authors.py
"""

import argparse
import sys
import time
from pathlib import Path

import duckdb


def load_schema(schema_path):
    """Load schema config from a schema file.

    The schema file must define a dict named *_SCHEMA (e.g., WORKS_SCHEMA,
    AUTHORS_SCHEMA) where keys are field names and values have a "type" key.

    Returns the schema dict.
    """
    import importlib.util
    spec = importlib.util.spec_from_file_location("schema_config", schema_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    for attr_name in dir(mod):
        if attr_name.endswith("_SCHEMA") and isinstance(getattr(mod, attr_name), dict):
            return getattr(mod, attr_name)

    print(f"ERROR: No *_SCHEMA dict found in {schema_path}")
    sys.exit(1)


# DuckDB type mapping
DUCKDB_TYPES = {
    "str": "VARCHAR",
    "int": "BIGINT",
    "float": "DOUBLE",
    "bool": "BOOLEAN",
    "json": "JSON",
}


def build_select_clause(schema):
    """Build SQL SELECT clause from schema config.

    Scalar fields are cast to their target type.
    JSON fields are serialized to valid JSON strings via to_json().
    """
    parts = []
    for field, config in schema.items():
        ftype = config["type"]
        if ftype == "json":
            # to_json() → valid JSON, CAST to VARCHAR so parquet stores as string not binary
            parts.append(f'CAST(to_json("{field}") AS VARCHAR) AS "{field}"')
        else:
            duckdb_type = DUCKDB_TYPES[ftype]
            parts.append(f'CAST("{field}" AS {duckdb_type}) AS "{field}"')
    return ",\n        ".join(parts)


def build_columns_spec(schema):
    """Build a DuckDB columns dict from schema config.

    Returns a SQL fragment like: {'id': 'VARCHAR', 'year': 'BIGINT', ...}
    This tells read_json exactly what columns to expect, so:
      - Missing columns in the data become NULL automatically
      - Extra columns in the data are ignored
      - No separate column detection step needed
    """
    pairs = []
    for field, config in schema.items():
        duckdb_type = DUCKDB_TYPES[config["type"]]
        pairs.append(f"'{field}': '{duckdb_type}'")
    return "{" + ", ".join(pairs) + "}"


def find_gz_glob(input_dir):
    """Build a glob pattern for DuckDB to find .gz files."""
    input_dir = Path(input_dir)
    # Check if gz files are directly in the dir or in subdirectories
    direct = list(input_dir.glob("*.gz"))
    nested = list(input_dir.glob("*/*.gz"))

    if direct and not nested:
        return str(input_dir / "*.gz")
    elif nested:
        return str(input_dir / "**" / "*.gz")
    elif direct:
        return str(input_dir / "*.gz")
    else:
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Parse OpenAlex gz JSON to parquet (DuckDB engine)"
    )
    parser.add_argument("--input", type=str, required=True,
                        help="Input directory containing .gz files (searched recursively)")
    parser.add_argument("--output", type=str, required=True,
                        help="Output parquet file path (e.g., data/intermediates/works.parquet)")
    parser.add_argument("--schema", type=str, required=True,
                        help="Path to schema config file (e.g., openalex_parse/schemas/works.py)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max records to parse (default: all)")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_path = Path(args.output)
    schema_path = Path(args.schema)

    # Load schema
    print(f"Loading schema from {schema_path}...")
    schema = load_schema(schema_path)

    # Find gz files
    gz_glob = find_gz_glob(input_dir)
    if gz_glob is None:
        print(f"ERROR: No .gz files found in {input_dir}")
        sys.exit(1)

    print(f"Input:          {gz_glob}")
    print(f"Output:         {output_path}")
    print(f"Schema fields:  {len(schema)}")
    if args.limit:
        print(f"Limit:          {args.limit:,}")
    print()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()

    con = duckdb.connect()

    # Build column spec from schema — tells DuckDB exactly what to expect
    # Missing columns in data → NULL, extra columns in data → ignored
    columns_spec = build_columns_spec(schema)
    select_clause = build_select_clause(schema)
    limit_clause = f"LIMIT {args.limit}" if args.limit else ""

    # Read gz JSON → extract fields → write parquet (streamed, memory safe)
    print("Parsing gz JSON → parquet via DuckDB...")
    query = f"""
        COPY (
            SELECT
                {select_clause}
            FROM read_json(
                '{gz_glob}',
                format = 'newline_delimited',
                columns = {columns_spec},
                maximum_object_size = 10485760
            )
            {limit_clause}
        ) TO '{output_path}' (FORMAT PARQUET)
    """
    con.execute(query)
    t1 = time.time()

    # Summary
    row_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
    ).fetchone()[0]
    file_size = output_path.stat().st_size / 1024 / 1024

    print(f"\n{'─' * 60}")
    print(f"SUMMARY")
    print(f"{'─' * 60}")
    print(f"  Records:    {row_count:,}")
    print(f"  Columns:    {len(schema)}")
    print(f"  Output:     {output_path}")
    print(f"  File size:  {file_size:.1f} MB")
    print(f"  Total time: {t1 - t0:.1f}s")

    con.close()


if __name__ == "__main__":
    main()
