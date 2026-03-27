"""
Create a flat work-citation table from works parquet.

One row per paper x referenced (cited) work.

Usage:
    python -m openalex_parse.derived.work_referenced_works \
        --input data/intermediates/works/*.parquet \
        --output data/intermediates/work_referenced_works.parquet
"""

import argparse
import time
from pathlib import Path

import polars as pl


def main():
    parser = argparse.ArgumentParser(
        description="Explode works parquet into work-citation table"
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
            pl.col("referenced_works").str.json_decode(pl.List(pl.Utf8)).alias("_refs"),
        )
        .filter(pl.col("_refs").is_not_null())
        .explode("_refs")
        .rename({"_refs": "referenced_work_id"})
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
