"""
Create a flat work-author-affiliation table from works parquet.

Explodes authorships and institutions arrays: one row per paper x author x
institution. Authors with multiple institutions produce multiple rows.
Authors with no institutions produce one row with NULL institution fields.

Grain: work_id + author_id + institution_id

Usage:
    python -m openalex_parse.derived.work_author_affiliations \
        --input data/intermediates/works.parquet \
        --output data/intermediates/work_author_affiliations.parquet
"""

import argparse
import time
from pathlib import Path

import duckdb


def main():
    parser = argparse.ArgumentParser(
        description="Explode works parquet into work-author-affiliation table"
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

    # Two-step unnest:
    #   1. Unnest authorships[] → one row per author
    #   2. Unnest institutions[] → one row per author × institution
    # Authors with empty institutions[] keep one row via LEFT JOIN LATERAL
    con.execute(f"""
        COPY (
            WITH authors AS (
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
                    a.raw_affiliation_strings,
                    a.institutions
                FROM read_parquet('{inp}') w,
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
                        "raw_affiliation_strings": "JSON",
                        "institutions": [{{
                            "id": "VARCHAR",
                            "display_name": "VARCHAR",
                            "ror": "VARCHAR",
                            "country_code": "VARCHAR",
                            "type": "VARCHAR",
                            "lineage": "JSON"
                        }}]
                    }}]')) AS a
                )
                WHERE w.authorships IS NOT NULL AND w.authorships != '[]'
            )
            SELECT
                a.work_id,
                a.doi,
                a.publication_year,
                a.author_position,
                a.raw_author_name,
                a.author_id,
                a.author_display_name,
                a.orcid,
                a.is_corresponding,
                CAST(a.countries AS VARCHAR) AS countries,
                CAST(a.raw_affiliation_strings AS VARCHAR) AS raw_affiliation_strings,
                i.id AS institution_id,
                i.display_name AS institution_display_name,
                i.ror AS institution_ror,
                i.country_code AS institution_country_code,
                i.type AS institution_type,
                CAST(i.lineage AS VARCHAR) AS institution_lineage
            FROM authors a
            LEFT JOIN LATERAL (
                SELECT UNNEST(a.institutions) AS i
            ) ON true
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
    preview = con.execute(
        f"SELECT * FROM read_parquet('{out}') LIMIT 5"
    ).fetchdf()
    print(preview.to_string())

    con.close()


if __name__ == "__main__":
    main()
