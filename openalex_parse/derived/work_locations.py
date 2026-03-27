"""
Unnest the locations array from works parquet.

One row per paper x location, with source details flattened.

Usage:
    python -m openalex_parse.derived.work_locations \
        --input data/intermediates/works.parquet \
        --output data/intermediates/work_locations.parquet
"""

import argparse
import time
from pathlib import Path

import duckdb


def main():
    parser = argparse.ArgumentParser(
        description="Unnest works locations into flat table"
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
                loc.is_oa,
                loc.is_published,
                loc.is_accepted,
                loc.landing_page_url,
                loc.pdf_url,
                loc.license,
                loc.license_id,
                loc.version,
                loc.provenance,
                loc.source.id AS source_id,
                loc.source.display_name AS source_name,
                loc.source.issn_l,
                loc.source.type AS source_type,
                loc.source.is_oa AS source_is_oa,
                loc.source.is_in_doaj AS source_is_in_doaj,
                loc.source.is_core AS source_is_core,
                loc.source.host_organization AS source_host_organization,
                loc.source.host_organization_name AS source_host_organization_name
            FROM read_parquet('{inp}'),
            LATERAL (
                SELECT UNNEST(from_json(locations, '[{{
                    "is_oa": "BOOLEAN",
                    "is_published": "BOOLEAN",
                    "is_accepted": "BOOLEAN",
                    "landing_page_url": "VARCHAR",
                    "pdf_url": "VARCHAR",
                    "license": "VARCHAR",
                    "license_id": "VARCHAR",
                    "version": "VARCHAR",
                    "provenance": "VARCHAR",
                    "source": {{
                        "id": "VARCHAR",
                        "display_name": "VARCHAR",
                        "issn_l": "VARCHAR",
                        "type": "VARCHAR",
                        "is_oa": "BOOLEAN",
                        "is_in_doaj": "BOOLEAN",
                        "is_core": "BOOLEAN",
                        "host_organization": "VARCHAR",
                        "host_organization_name": "VARCHAR"
                    }}
                }}]')) AS loc
            )
            WHERE locations IS NOT NULL AND locations != '[]'
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
