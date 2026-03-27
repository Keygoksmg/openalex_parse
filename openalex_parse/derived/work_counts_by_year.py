"""
Unnest the counts_by_year array from works parquet.

One row per paper x year with citation count.

Usage:
    python -m openalex_parse.derived.work_counts_by_year \
        --input data/intermediates/works/*.parquet \
        --output data/intermediates/work_counts_by_year.parquet
"""

import argparse
import time
from pathlib import Path

import polars as pl


def main():
    parser = argparse.ArgumentParser(
        description="Unnest works counts_by_year into flat table"
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
            pl.col("counts_by_year").str.json_decode(
                pl.List(pl.Struct({
                    "year": pl.Int64,
                    "cited_by_count": pl.Int64,
                }))
            ).alias("_counts"),
        )
        .filter(pl.col("_counts").is_not_null())
        .explode("_counts")
        .with_columns(
            pl.col("_counts").struct.field("year").alias("count_year"),
            pl.col("_counts").struct.field("cited_by_count"),
        )
        .drop("_counts")
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
