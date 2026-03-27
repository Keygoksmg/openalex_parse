"""
Partition-parallel parser for OpenAlex gz JSON into parquet using DuckDB.

Same as parse.py but processes each input partition separately, writing
one parquet file per partition to an output directory. Benefits:
  - Constant memory (~4-8 GB regardless of total data size)
  - Progress tracking (partition N/M)
  - No single-file write bottleneck
  - Resilient to interruption (completed partitions are kept)

Output is a directory of parquet files, readable as one table:
    read_parquet('output_dir/*.parquet')

Usage:
    python -m openalex_parse.parse_partitioned \
        --input /path/to/openalex/data/works \
        --output /path/to/output/works \
        --schema openalex_parse/schemas/works.py
"""

import argparse
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import duckdb

from openalex_parse.parse import (
    load_schema,
    build_select_clause,
    build_columns_spec,
    _escape_sql_string,
)


def find_partitions(input_dir):
    """Find all partition directories or return the input dir itself."""
    input_dir = Path(input_dir)

    # Check for partition subdirectories (updated_date=...)
    partitions = sorted(
        [d for d in input_dir.iterdir()
         if d.is_dir() and d.name.startswith("updated_date=")]
    )
    if partitions:
        return partitions

    # No partition dirs — check if gz files are directly in input_dir
    if list(input_dir.glob("*.gz")):
        return [input_dir]

    return []


def _process_partition(args_tuple):
    """Process a single partition (runs in worker process)."""
    partition_dir, out_file, select_clause, columns_spec, limit_clause = args_tuple
    gz_glob = str(partition_dir / "*.gz")
    gz_safe = _escape_sql_string(gz_glob)
    out_safe = _escape_sql_string(out_file)

    t0 = time.time()
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT {select_clause}
            FROM read_json(
                '{gz_safe}',
                format = 'newline_delimited',
                columns = {columns_spec},
                maximum_object_size = 10485760
            )
            {limit_clause}
        ) TO '{out_safe}' (FORMAT PARQUET)
    """)
    row_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_safe}')"
    ).fetchone()[0]
    con.close()

    file_size = out_file.stat().st_size
    elapsed = time.time() - t0
    return partition_dir.name, row_count, file_size, elapsed


def main():
    parser = argparse.ArgumentParser(
        description="Parse OpenAlex gz JSON to partitioned parquet (one file per partition)"
    )
    parser.add_argument("--input", type=str, required=True,
                        help="Input directory containing partition subdirs with .gz files")
    parser.add_argument("--output", type=str, required=True,
                        help="Output directory for parquet files")
    parser.add_argument("--schema", type=str, required=True,
                        help="Path to schema config file")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max records per partition (default: all)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel workers (default: 1 = sequential)")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_dir = Path(args.output)
    schema_path = Path(args.schema)

    # Load schema
    print(f"Loading schema from {schema_path}...")
    schema = load_schema(schema_path)
    select_clause = build_select_clause(schema)
    columns_spec = build_columns_spec(schema)

    # Find partitions
    partitions = find_partitions(input_dir)
    if not partitions:
        print(f"ERROR: No partitions or .gz files found in {input_dir}")
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    limit_clause = f"LIMIT {args.limit}" if args.limit else ""

    print(f"Input:       {input_dir}")
    print(f"Output:      {output_dir}")
    print(f"Schema:      {len(schema)} fields")
    print(f"Partitions:  {len(partitions)}")
    print(f"Workers:     {args.workers}")
    print()

    # Filter out already-processed partitions (resume support)
    todo = []
    skipped = 0
    for partition_dir in partitions:
        out_file = output_dir / f"{partition_dir.name}.parquet"
        if out_file.exists():
            skipped += 1
        else:
            todo.append((partition_dir, out_file, select_clause, columns_spec, limit_clause))

    if skipped:
        print(f"Skipping {skipped} already-processed partitions")
        print()

    t0 = time.time()
    total_rows = 0
    total_bytes = 0
    done = 0
    total_todo = len(todo)

    if args.workers <= 1:
        # Sequential
        for task in todo:
            part_name, row_count, file_size, elapsed = _process_partition(task)
            done += 1
            total_rows += row_count
            total_bytes += file_size
            wall = time.time() - t0
            rate = total_rows / wall if wall > 0 else 0
            print(f"[{done}/{total_todo}] {part_name} — "
                  f"{row_count:,} rows, {file_size/1024/1024:.0f} MB, "
                  f"{elapsed:.1f}s "
                  f"(total: {total_rows:,} rows, {wall:.0f}s, {rate:,.0f} rows/s)")
    else:
        # Parallel
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_process_partition, task): task for task in todo}
            for future in as_completed(futures):
                part_name, row_count, file_size, elapsed = future.result()
                done += 1
                total_rows += row_count
                total_bytes += file_size
                wall = time.time() - t0
                rate = total_rows / wall if wall > 0 else 0
                print(f"[{done}/{total_todo}] {part_name} — "
                      f"{row_count:,} rows, {file_size/1024/1024:.0f} MB, "
                      f"{elapsed:.1f}s "
                      f"(total: {total_rows:,} rows, {wall:.0f}s, {rate:,.0f} rows/s)")

    t1 = time.time()

    print(f"\n{'─' * 60}")
    print(f"SUMMARY")
    print(f"{'─' * 60}")
    print(f"  Partitions: {len(partitions)} ({skipped} skipped, {total_todo} processed)")
    print(f"  Workers:    {args.workers}")
    print(f"  Records:    {total_rows:,}")
    print(f"  Output:     {output_dir}")
    print(f"  File size:  {total_bytes/1024/1024:.1f} MB")
    print(f"  Total time: {t1 - t0:.1f}s")


if __name__ == "__main__":
    main()
