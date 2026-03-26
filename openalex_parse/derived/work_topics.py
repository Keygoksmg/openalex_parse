"""
Create a paper-topics table by exploding the topics JSON from works parquet.

One row per paper × topic, with the full topic hierarchy.

Usage:
    python -m openalex_parse.derived.work_topics \
        --input data/intermediates/works.parquet \
        --output data/intermediates/work_topics.parquet
"""

import argparse
import time
from pathlib import Path

import duckdb
import polars as pl


def main():
    parser = argparse.ArgumentParser(
        description="Explode works parquet into paper-topics table"
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

    # Escape paths for safe SQL interpolation
    inp = str(input_path).replace("'", "''")
    out = str(output_path).replace("'", "''")

    con.execute(f"""
        COPY (
            SELECT
                id AS work_id,
                t.id AS topic_id,
                t.display_name AS topic_display_name,
                t.score AS topic_score,
                t.subfield.display_name AS subfield,
                t.field.display_name AS field,
                t.domain.display_name AS domain
            FROM read_parquet('{inp}'),
            LATERAL (
                SELECT UNNEST(from_json(topics, '[{{
                    "id": "VARCHAR",
                    "display_name": "VARCHAR",
                    "score": "DOUBLE",
                    "subfield": {{"display_name": "VARCHAR"}},
                    "field": {{"display_name": "VARCHAR"}},
                    "domain": {{"display_name": "VARCHAR"}}
                }}]')) AS t
            )
            WHERE topics IS NOT NULL AND topics != '[]'
        ) TO '{out}' (FORMAT PARQUET)
    """)

    t1 = time.time()

    # Summary
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

    # Preview
    print()
    print(pl.read_parquet(output_path).head(5))

    con.close()


if __name__ == "__main__":
    main()
