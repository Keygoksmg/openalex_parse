"""
Create a flat work-author-affiliation table from works parquet.

Explodes authorships and institutions arrays: one row per paper x author x
institution. Authors with multiple institutions produce multiple rows.
Authors with no institutions produce one row with NULL institution fields.

Grain: work_id + author_id + institution_id

Usage:
    python -m openalex_parse.derived.work_author_affiliations \
        --input data/intermediates/works/*.parquet \
        --output data/intermediates/work_author_affiliations.parquet
"""

import argparse
import time
from pathlib import Path

import polars as pl


AUTHORSHIP_DTYPE = pl.List(pl.Struct({
    "author_position": pl.Utf8,
    "raw_author_name": pl.Utf8,
    "author": pl.Struct({
        "id": pl.Utf8,
        "display_name": pl.Utf8,
        "orcid": pl.Utf8,
    }),
    "is_corresponding": pl.Boolean,
    "countries": pl.List(pl.Utf8),
    "raw_affiliation_strings": pl.List(pl.Utf8),
    "institutions": pl.List(pl.Struct({
        "id": pl.Utf8,
        "display_name": pl.Utf8,
        "ror": pl.Utf8,
        "country_code": pl.Utf8,
        "type": pl.Utf8,
        "lineage": pl.List(pl.Utf8),
    })),
}))


def main():
    parser = argparse.ArgumentParser(
        description="Explode works parquet into work-author-affiliation table"
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

    # Step 1: Parse authorships JSON and explode to one row per author
    authors = (
        lf.select(
            pl.col("id").alias("work_id"),
            pl.col("doi"),
            pl.col("publication_year"),
            pl.col("authorships").str.json_decode(AUTHORSHIP_DTYPE).alias("_auths"),
        )
        .filter(pl.col("_auths").is_not_null())
        .explode("_auths")
        .with_columns(
            pl.col("_auths").struct.field("author_position"),
            pl.col("_auths").struct.field("raw_author_name"),
            pl.col("_auths").struct.field("author").struct.field("id").alias("author_id"),
            pl.col("_auths").struct.field("author").struct.field("display_name").alias("author_display_name"),
            pl.col("_auths").struct.field("author").struct.field("orcid"),
            pl.col("_auths").struct.field("is_corresponding"),
            pl.col("_auths").struct.field("countries").list.join(", ").alias("countries"),
            pl.col("_auths").struct.field("raw_affiliation_strings").list.join(" | ").alias("raw_affiliation_strings"),
            pl.col("_auths").struct.field("institutions").alias("_institutions"),
        )
        .drop("_auths")
    )

    # Step 2: Explode institutions — one row per author x institution
    result = (
        authors
        .explode("_institutions")
        .with_columns(
            pl.col("_institutions").struct.field("id").alias("institution_id"),
            pl.col("_institutions").struct.field("display_name").alias("institution_display_name"),
            pl.col("_institutions").struct.field("ror").alias("institution_ror"),
            pl.col("_institutions").struct.field("country_code").alias("institution_country_code"),
            pl.col("_institutions").struct.field("type").alias("institution_type"),
            pl.col("_institutions").struct.field("lineage").list.join(", ").alias("institution_lineage"),
        )
        .drop("_institutions")
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
