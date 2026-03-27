"""
Unnest the counts_by_year array from works parquet.

One row per paper x year with citation count.

Usage:
    python -m openalex_parse.derived.work_counts_by_year \
        --input data/intermediates/works.parquet \
        --output data/intermediates/work_counts_by_year.parquet
"""

import argparse
import time
from pathlib import Path

import duckdb


def main():
    parser = argparse.ArgumentParser(
        description="Unnest works counts_by_year into flat table"
    )
    parser.add_argument("--input", type=str, required=True,
                        help="Input works parquet file")
    parser.add_argument("--output", type=str, required=True,
                        help="Output parquet file")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print()

    t0 = time.time()
    con = duckdb.connect()

    inp = str(input_path).replace("'", "''")
    out = str(output_path).replace("'", "''")

    con.execute(f"""
        COPY (
            SELECT
                id AS work_id,
                publication_year,
                c.year AS count_year,
                c.cited_by_count
            FROM read_parquet('{inp}'),
            LATERAL (
                SELECT UNNEST(from_json(counts_by_year, '[{{
                    "year": "BIGINT",
                    "cited_by_count": "BIGINT"
                }}]')) AS c
            )
            WHERE counts_by_year IS NOT NULL AND counts_by_year != '[]'
        ) TO '{out}' (FORMAT PARQUET)
    """)

    t1 = time.time()

    row_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out}')"
    ).fetchone()[0]
    file_size = output_path.stat().st_size / 1024 / 1024

    print(f"{'─' * 60}")
    print(f"SUMMARY")
    print(f"{'─' * 60}")
    print(f"  Records:    {row_count:,}")
    print(f"  Output:     {output_path}")
    print(f"  File size:  {file_size:.2f} MB")
    print(f"  Total time: {t1 - t0:.1f}s")

    con.close()


if __name__ == "__main__":
    main()
