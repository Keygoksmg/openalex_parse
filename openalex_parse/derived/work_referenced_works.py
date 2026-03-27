"""
Create a flat work-citation table from works parquet.

One row per paper x referenced (cited) work.

Usage:
    python -m openalex_parse.derived.work_referenced_works \
        --input data/intermediates/works.parquet \
        --output data/intermediates/work_referenced_works.parquet
"""

import argparse
import time
from pathlib import Path

import duckdb


def main():
    parser = argparse.ArgumentParser(
        description="Explode works parquet into work-citation table"
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
                UNNEST(from_json(referenced_works, '["VARCHAR"]')) AS referenced_work_id
            FROM read_parquet('{inp}')
            WHERE referenced_works IS NOT NULL AND referenced_works != '[]'
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
