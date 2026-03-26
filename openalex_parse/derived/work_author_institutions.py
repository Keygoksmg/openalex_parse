"""
Create a flat work-author-institution table from works parquet.

Explodes the authorships JSON array: one row per paper x author, with
the first institution's details as representative affiliation.

Usage:
    python -m openalex_parse.derived.work_author_institutions \
        --input data/intermediates/works.parquet \
        --output data/intermediates/work_author_institutions.parquet
"""

import argparse
import time
from pathlib import Path

import duckdb
import polars as pl


def main():
    parser = argparse.ArgumentParser(
        description="Explode works parquet into work-author-institution table"
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

    con.execute(f"""
        COPY (
            SELECT
                w.id AS work_id,
                w.doi,
                w.publication_year,
                a.author_position,
                a.raw_author_name,
                a.author.id AS author_id,
                a.author.display_name AS author_display_name,
                a.author.orcid AS orcid,
                a.is_corresponding,
                a.countries,
                -- First institution as representative affiliation
                a.institutions[1].id AS first_institution_id,
                a.institutions[1].display_name AS first_institution_display_name,
                a.institutions[1].ror AS first_institution_ror,
                a.institutions[1].country_code AS first_institution_country_code,
                a.institutions[1].type AS first_institution_type
            FROM read_parquet('{input_path}') w,
            LATERAL (
                SELECT UNNEST(from_json(w.authorships, '[{{
                    "author_position": "VARCHAR",
                    "raw_author_name": "VARCHAR",
                    "author": {{
                        "id": "VARCHAR",
                        "display_name": "VARCHAR",
                        "orcid": "VARCHAR"
                    }},
                    "is_corresponding": "BOOLEAN",
                    "countries": "JSON",
                    "institutions": [{{
                        "id": "VARCHAR",
                        "display_name": "VARCHAR",
                        "ror": "VARCHAR",
                        "country_code": "VARCHAR",
                        "type": "VARCHAR"
                    }}]
                }}]')) AS a
            )
            WHERE w.authorships IS NOT NULL AND w.authorships != '[]'
        ) TO '{output_path}' (FORMAT PARQUET)
    """)

    t1 = time.time()

    # Summary
    row_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_path}')"
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
