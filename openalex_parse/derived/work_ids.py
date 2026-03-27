"""
Unnest the ids object from works parquet into a flat table.

One row per paper with all identifier types as separate columns.

Usage:
    python -m openalex_parse.derived.work_ids \
        --input data/intermediates/works/*.parquet \
        --output data/intermediates/work_ids.parquet
"""

import argparse
import time
from pathlib import Path

import polars as pl


def main():
    parser = argparse.ArgumentParser(
        description="Unnest work IDs into flat columns"
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
        pl.col("ids").str.json_decode(
            pl.Struct({
                "openalex": pl.Utf8,
                "doi": pl.Utf8,
                "mag": pl.Utf8,
                "pmid": pl.Utf8,
                "pmcid": pl.Utf8,
            })
        ).alias("_ids"),
    ).with_columns(
        pl.col("_ids").struct.field("openalex"),
        pl.col("_ids").struct.field("doi").alias("ids_doi"),
        pl.col("_ids").struct.field("mag"),
        pl.col("_ids").struct.field("pmid"),
        pl.col("_ids").struct.field("pmcid"),
    ).drop("_ids")

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
