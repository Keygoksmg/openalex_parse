"""
Unnest the ids object from works parquet into a flat table.

One row per paper with all identifier types as separate columns.

Usage:
    python -m openalex_parse.derived.work_ids \
        --input data/intermediates/works.parquet \
        --output data/intermediates/work_ids.parquet
"""

import argparse
import time
from pathlib import Path

import duckdb


def main():
    parser = argparse.ArgumentParser(
        description="Unnest work IDs into flat columns"
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
                doi,
                i.openalex,
                i.doi AS ids_doi,
                i.mag,
                i.pmid,
                i.pmcid
            FROM read_parquet('{inp}'),
            LATERAL (
                SELECT from_json(ids, '{{
                    "openalex": "VARCHAR",
                    "doi": "VARCHAR",
                    "mag": "VARCHAR",
                    "pmid": "VARCHAR",
                    "pmcid": "VARCHAR"
                }}') AS i
            )
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
