"""
Create a paper-topics table by exploding the topics JSON from works parquet.

One row per paper x topic, with the full topic hierarchy.

Usage:
    python -m openalex_parse.derived.work_topics \
        --input data/intermediates/works/*.parquet \
        --output data/intermediates/work_topics.parquet
"""

import argparse
import time
from pathlib import Path

import polars as pl


def main():
    parser = argparse.ArgumentParser(
        description="Explode works parquet into paper-topics table"
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
            pl.col("topics").str.json_decode(
                pl.List(pl.Struct({
                    "id": pl.Utf8,
                    "display_name": pl.Utf8,
                    "score": pl.Float64,
                    "subfield": pl.Struct({"display_name": pl.Utf8}),
                    "field": pl.Struct({"display_name": pl.Utf8}),
                    "domain": pl.Struct({"display_name": pl.Utf8}),
                }))
            ).alias("_topics"),
        )
        .filter(pl.col("_topics").is_not_null())
        .explode("_topics")
        .with_columns(
            pl.col("_topics").struct.field("id").alias("topic_id"),
            pl.col("_topics").struct.field("display_name").alias("topic_display_name"),
            pl.col("_topics").struct.field("score").alias("topic_score"),
            pl.col("_topics").struct.field("subfield").struct.field("display_name").alias("subfield"),
            pl.col("_topics").struct.field("field").struct.field("display_name").alias("field"),
            pl.col("_topics").struct.field("domain").struct.field("display_name").alias("domain"),
        )
        .drop("_topics")
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
