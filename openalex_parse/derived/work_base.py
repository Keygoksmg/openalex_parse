"""
Create a base paper-level table with commonly used fields unnested.

One row per paper with scalar fields + key nested objects (primary_topic,
open_access, primary_location) flattened into columns. No JSON string
columns — everything is a typed scalar ready for analysis.

Usage:
    python -m openalex_parse.derived.work_base \
        --input data/intermediates/works.parquet \
        --output data/intermediates/work_base.parquet
"""

import argparse
import time
from pathlib import Path

import duckdb


def main():
    parser = argparse.ArgumentParser(
        description="Create base paper-level table from works parquet"
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
            WITH base AS (
                SELECT
                    id AS work_id,
                    doi,
                    publication_year,
                    publication_date,
                    language,
                    type,
                    cited_by_count,
                    fwci,
                    referenced_works_count,
                    authors_count,
                    institutions_distinct_count,
                    countries_distinct_count,
                    is_retracted,
                    is_paratext,
                    has_fulltext,
                    pt.id AS primary_topic_id,
                    pt.display_name AS primary_topic_name,
                    pt.score AS primary_topic_score,
                    pt.subfield.id AS primary_subfield_id,
                    pt.subfield.display_name AS primary_subfield_name,
                    pt.field.id AS primary_field_id,
                    pt.field.display_name AS primary_field_name,
                    pt.domain.id AS primary_domain_id,
                    pt.domain.display_name AS primary_domain_name,
                    oa.is_oa,
                    oa.oa_status,
                    oa.oa_url,
                    pl.source.id AS source_id,
                    pl.source.display_name AS source_name,
                    pl.source.type AS source_type,
                    pl.source.issn_l,
                    pl.source.host_organization_name
                FROM read_parquet('{inp}'),
                LATERAL (
                    SELECT from_json(primary_topic, '{{
                        "id": "VARCHAR",
                        "display_name": "VARCHAR",
                        "score": "DOUBLE",
                        "subfield": {{"id": "VARCHAR", "display_name": "VARCHAR"}},
                        "field": {{"id": "VARCHAR", "display_name": "VARCHAR"}},
                        "domain": {{"id": "VARCHAR", "display_name": "VARCHAR"}}
                    }}') AS pt
                ),
                LATERAL (
                    SELECT from_json(open_access, '{{
                        "is_oa": "BOOLEAN",
                        "oa_status": "VARCHAR",
                        "oa_url": "VARCHAR"
                    }}') AS oa
                ),
                LATERAL (
                    SELECT from_json(primary_location, '{{
                        "source": {{
                            "id": "VARCHAR",
                            "display_name": "VARCHAR",
                            "type": "VARCHAR",
                            "issn_l": "VARCHAR",
                            "host_organization_name": "VARCHAR"
                        }}
                    }}') AS pl
                )
            )
            SELECT
                *,
                -- Field-normalized citation: cited_by_count / mean(cited_by_count) per year × field
                cited_by_count * 1.0 / NULLIF(AVG(cited_by_count) OVER (
                    PARTITION BY publication_year, primary_field_id
                ), 0) AS cf,
                -- Subfield-normalized citation: cited_by_count / mean(cited_by_count) per year × subfield
                cited_by_count * 1.0 / NULLIF(AVG(cited_by_count) OVER (
                    PARTITION BY publication_year, primary_subfield_id
                ), 0) AS cf_sub
            FROM base
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

    # Preview
    print()
    preview = con.execute(
        f"SELECT * FROM read_parquet('{out}') LIMIT 5"
    ).fetchdf()
    print(preview.to_string())

    con.close()


if __name__ == "__main__":
    main()
