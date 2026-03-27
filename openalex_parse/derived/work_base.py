"""
Create a base paper-level table with commonly used fields unnested.

One row per paper with scalar fields + key nested objects (primary_topic,
open_access, primary_location) flattened into columns. No JSON string
columns — everything is a typed scalar ready for analysis.

Uses Polars lazy scan → sink_parquet for streaming, constant-memory processing.

Usage:
    python -m openalex_parse.derived.work_base \
        --input data/intermediates/works/*.parquet \
        --output data/intermediates/work_base.parquet
"""

import argparse
import time
from pathlib import Path

import polars as pl


def main():
    parser = argparse.ArgumentParser(
        description="Create base paper-level table from works parquet"
    )
    parser.add_argument("--input", type=str, required=True,
                        help="Input works parquet (file or glob)")
    parser.add_argument("--output", type=str, required=True,
                        help="Output parquet file")
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Input:  {args.input}")
    print(f"Output: {output_path}")
    print()

    t0 = time.time()

    lf = pl.scan_parquet(args.input)

    result = lf.select(
        pl.col("id").alias("work_id"),
        pl.col("doi"),
        pl.col("publication_year"),
        pl.col("publication_date"),
        pl.col("language"),
        pl.col("type"),
        pl.col("cited_by_count"),
        pl.col("fwci"),
        pl.col("referenced_works_count"),
        pl.col("authors_count"),
        pl.col("institutions_distinct_count"),
        pl.col("countries_distinct_count"),
        pl.col("is_retracted"),
        pl.col("is_paratext"),
        pl.col("has_fulltext"),
        # Primary topic
        pl.col("primary_topic").str.json_decode(
            pl.Struct({
                "id": pl.Utf8,
                "display_name": pl.Utf8,
                "score": pl.Float64,
                "subfield": pl.Struct({"id": pl.Utf8, "display_name": pl.Utf8}),
                "field": pl.Struct({"id": pl.Utf8, "display_name": pl.Utf8}),
                "domain": pl.Struct({"id": pl.Utf8, "display_name": pl.Utf8}),
            })
        ).alias("_pt"),
        # Open access
        pl.col("open_access").str.json_decode(
            pl.Struct({
                "is_oa": pl.Boolean,
                "oa_status": pl.Utf8,
                "oa_url": pl.Utf8,
            })
        ).alias("_oa"),
        # Primary location
        pl.col("primary_location").str.json_decode(
            pl.Struct({
                "source": pl.Struct({
                    "id": pl.Utf8,
                    "display_name": pl.Utf8,
                    "type": pl.Utf8,
                    "issn_l": pl.Utf8,
                    "host_organization_name": pl.Utf8,
                }),
            })
        ).alias("_pl"),
    ).with_columns(
        pl.col("_pt").struct.field("id").alias("primary_topic_id"),
        pl.col("_pt").struct.field("display_name").alias("primary_topic_name"),
        pl.col("_pt").struct.field("score").alias("primary_topic_score"),
        pl.col("_pt").struct.field("subfield").struct.field("id").alias("primary_subfield_id"),
        pl.col("_pt").struct.field("subfield").struct.field("display_name").alias("primary_subfield_name"),
        pl.col("_pt").struct.field("field").struct.field("id").alias("primary_field_id"),
        pl.col("_pt").struct.field("field").struct.field("display_name").alias("primary_field_name"),
        pl.col("_pt").struct.field("domain").struct.field("id").alias("primary_domain_id"),
        pl.col("_pt").struct.field("domain").struct.field("display_name").alias("primary_domain_name"),
        pl.col("_oa").struct.field("is_oa"),
        pl.col("_oa").struct.field("oa_status"),
        pl.col("_oa").struct.field("oa_url"),
        pl.col("_pl").struct.field("source").struct.field("id").alias("source_id"),
        pl.col("_pl").struct.field("source").struct.field("display_name").alias("source_name"),
        pl.col("_pl").struct.field("source").struct.field("type").alias("source_type"),
        pl.col("_pl").struct.field("source").struct.field("issn_l"),
        pl.col("_pl").struct.field("source").struct.field("host_organization_name"),
    ).drop("_pt", "_oa", "_pl")

    print("Sinking to parquet (streaming)...")
    result.sink_parquet(output_path)

    t1 = time.time()
    file_size = output_path.stat().st_size / 1024 / 1024

    print(f"\n{'─' * 60}")
    print(f"SUMMARY")
    print(f"{'─' * 60}")
    print(f"  Output:     {output_path}")
    print(f"  File size:  {file_size:.2f} MB")
    print(f"  Total time: {t1 - t0:.1f}s")


if __name__ == "__main__":
    main()
