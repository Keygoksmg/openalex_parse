"""
Reconstruct paper titles and abstracts from works parquet.

The abstract_inverted_index field stores word->position mappings as JSON:
    {"This": [0], "is": [1], "a": [2], "test": [3]}

This script inverts the index back into readable text and outputs a flat table
with one row per paper: (work_id, doi, title, abstract).

Uses Polars scan_parquet with collect(streaming=True) for memory efficiency.
The map_elements UDF prevents full sink_parquet streaming, so we collect
in streaming mode instead.

Usage:
    python -m openalex_parse.derived.work_title_abstracts \
        --input data/intermediates/works/*.parquet \
        --output data/intermediates/work_title_abstracts.parquet
"""

import argparse
import json
import time
from pathlib import Path

import polars as pl


def reconstruct_abstract(inverted_index_str):
    """Reconstruct abstract text from an inverted index JSON string."""
    if not inverted_index_str or inverted_index_str in ("null", "{}"):
        return None
    try:
        inv_index = json.loads(inverted_index_str)
    except (json.JSONDecodeError, TypeError):
        return None
    if not inv_index:
        return None

    words = {}
    for word, positions in inv_index.items():
        for pos in positions:
            words[pos] = word

    if not words:
        return None

    max_pos = max(words.keys())
    if max_pos > 100_000:
        return None
    return " ".join(words.get(i, "") for i in range(max_pos + 1))


def main():
    parser = argparse.ArgumentParser(
        description="Reconstruct abstracts from works parquet"
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
        pl.col("title"),
        pl.col("abstract_inverted_index")
          .map_elements(reconstruct_abstract, return_dtype=pl.Utf8)
          .alias("abstract"),
    )

    print("Collecting with streaming mode...")
    df = result.collect(streaming=True)

    print(f"  Rows: {df.shape[0]:,}")
    df.write_parquet(output_path)

    t1 = time.time()

    n_total = df.shape[0]
    n_with_abstract = df.filter(pl.col("abstract").is_not_null()).shape[0]
    file_size = output_path.stat().st_size / 1024 / 1024
    pct = n_with_abstract / n_total * 100 if n_total > 0 else 0

    print(f"\n{'─' * 60}")
    print(f"SUMMARY")
    print(f"{'─' * 60}")
    print(f"  Records:          {n_total:,}")
    print(f"  With abstract:    {n_with_abstract:,} ({pct:.1f}%)")
    print(f"  Output:           {output_path}")
    print(f"  File size:        {file_size:.2f} MB")
    print(f"  Total time:       {t1 - t0:.1f}s")


if __name__ == "__main__":
    main()
