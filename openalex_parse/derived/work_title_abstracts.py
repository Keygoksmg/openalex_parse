"""
Reconstruct paper titles and abstracts from works parquet.

The abstract_inverted_index field stores word→position mappings as JSON:
    {"This": [0], "is": [1], "a": [2], "test": [3]}

This script inverts the index back into readable text and outputs a flat table
with one row per paper: (work_id, doi, title, abstract).

Usage:
    python -m openalex_parse.derived.work_title_abstracts \
        --input data/intermediates/works.parquet \
        --output data/intermediates/work_title_abstracts.parquet
"""

import argparse
import json
import time
from pathlib import Path

import polars as pl


def reconstruct_abstract(inverted_index_str):
    """Reconstruct abstract text from an inverted index JSON string.

    The inverted index maps each word to a list of positions:
        {"word1": [0, 5], "word2": [1, 3], ...}

    Returns the reconstructed abstract string, or None if input is null/empty.
    """
    if not inverted_index_str or inverted_index_str in ("null", "{}"):
        return None
    try:
        inv_index = json.loads(inverted_index_str)
    except (json.JSONDecodeError, TypeError):
        return None
    if not inv_index:
        return None

    # Build position → word mapping
    words = {}
    for word, positions in inv_index.items():
        for pos in positions:
            words[pos] = word

    if not words:
        return None

    # Reconstruct by sorting positions
    max_pos = max(words.keys())
    return " ".join(words.get(i, "") for i in range(max_pos + 1))


def main():
    parser = argparse.ArgumentParser(
        description="Reconstruct abstracts from works parquet"
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

    # Read only needed columns
    df = pl.read_parquet(
        input_path,
        columns=["id", "doi", "title", "abstract_inverted_index"],
    )
    print(f"Loaded {df.shape[0]:,} records")

    # Reconstruct abstracts using map_elements
    result = df.select(
        pl.col("id").alias("work_id"),
        pl.col("doi"),
        pl.col("title"),
        pl.col("abstract_inverted_index")
          .map_elements(reconstruct_abstract, return_dtype=pl.Utf8)
          .alias("abstract"),
    )

    result.write_parquet(output_path)

    t1 = time.time()

    # Summary
    n_total = result.shape[0]
    n_with_abstract = result.filter(pl.col("abstract").is_not_null()).shape[0]
    file_size = output_path.stat().st_size / 1024 / 1024

    print(f"\n{'─' * 60}")
    print(f"SUMMARY")
    print(f"{'─' * 60}")
    print(f"  Records:          {n_total:,}")
    pct = n_with_abstract / n_total * 100 if n_total > 0 else 0
    print(f"  With abstract:    {n_with_abstract:,} ({pct:.1f}%)")
    print(f"  Output:           {output_path}")
    print(f"  File size:        {file_size:.2f} MB")
    print(f"  Total time:       {t1 - t0:.1f}s")

    # Preview
    print()
    preview = result.filter(pl.col("abstract").is_not_null()).head(3)
    for row in preview.iter_rows(named=True):
        print(f"  ID: {row['work_id']}")
        print(f"  Title: {row['title']}")
        abstract = row["abstract"]
        if abstract and len(abstract) > 200:
            abstract = abstract[:200] + "..."
        print(f"  Abstract: {abstract}")
        print()


if __name__ == "__main__":
    main()
