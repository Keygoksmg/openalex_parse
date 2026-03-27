"""
Unnest the locations array from works parquet.

One row per paper x location, with source details flattened.

Usage:
    python -m openalex_parse.derived.work_locations \
        --input data/intermediates/works/*.parquet \
        --output data/intermediates/work_locations.parquet
"""

import argparse
import time
from pathlib import Path

import polars as pl


def main():
    parser = argparse.ArgumentParser(
        description="Unnest works locations into flat table"
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

    result = (
        lf.select(
            pl.col("id").alias("work_id"),
            pl.col("publication_year"),
            pl.col("locations").str.json_decode(
                pl.List(pl.Struct({
                    "is_oa": pl.Boolean,
                    "is_published": pl.Boolean,
                    "is_accepted": pl.Boolean,
                    "landing_page_url": pl.Utf8,
                    "pdf_url": pl.Utf8,
                    "license": pl.Utf8,
                    "license_id": pl.Utf8,
                    "version": pl.Utf8,
                    "provenance": pl.Utf8,
                    "source": pl.Struct({
                        "id": pl.Utf8,
                        "display_name": pl.Utf8,
                        "issn_l": pl.Utf8,
                        "type": pl.Utf8,
                        "is_oa": pl.Boolean,
                        "is_in_doaj": pl.Boolean,
                        "is_core": pl.Boolean,
                        "host_organization": pl.Utf8,
                        "host_organization_name": pl.Utf8,
                    }),
                }))
            ).alias("_locs"),
        )
        .filter(pl.col("_locs").is_not_null())
        .explode("_locs")
        .with_columns(
            pl.col("_locs").struct.field("is_oa"),
            pl.col("_locs").struct.field("is_published"),
            pl.col("_locs").struct.field("is_accepted"),
            pl.col("_locs").struct.field("landing_page_url"),
            pl.col("_locs").struct.field("pdf_url"),
            pl.col("_locs").struct.field("license"),
            pl.col("_locs").struct.field("license_id"),
            pl.col("_locs").struct.field("version"),
            pl.col("_locs").struct.field("provenance"),
            pl.col("_locs").struct.field("source").struct.field("id").alias("source_id"),
            pl.col("_locs").struct.field("source").struct.field("display_name").alias("source_name"),
            pl.col("_locs").struct.field("source").struct.field("issn_l"),
            pl.col("_locs").struct.field("source").struct.field("type").alias("source_type"),
            pl.col("_locs").struct.field("source").struct.field("is_oa").alias("source_is_oa"),
            pl.col("_locs").struct.field("source").struct.field("is_in_doaj").alias("source_is_in_doaj"),
            pl.col("_locs").struct.field("source").struct.field("is_core").alias("source_is_core"),
            pl.col("_locs").struct.field("source").struct.field("host_organization").alias("source_host_organization"),
            pl.col("_locs").struct.field("source").struct.field("host_organization_name").alias("source_host_organization_name"),
        )
        .drop("_locs")
    )

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
