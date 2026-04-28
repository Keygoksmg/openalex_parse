"""
Reconstruct paper titles and abstracts from works parquet.

The abstract_inverted_index field stores word->position mappings as JSON:
    {"This": [0], "is": [1], "a": [2], "test": [3]}

This script inverts the index back into readable text and outputs a flat table
with one row per paper: (work_id, doi, title, abstract).

Processes each input parquet file independently using a Rust Polars plugin for
native-speed JSON reconstruction. Output is a directory of parquet files.

Usage:
    python -m openalex_parse.derived.work_title_abstracts \
        --input data/intermediates/works/*.parquet \
        --output data/intermediates/work_title_abstracts/
"""

import argparse
import gc
import glob
import json
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl

# Rust plugin for native-speed abstract reconstruction
from polars_plugin import reconstruct_abstract as _reconstruct_abstract_expr


def reconstruct_abstract(inverted_index_str):
    """Reconstruct abstract text from an inverted index JSON string.

    Pure-Python fallback, used in tests and for small-scale work.
    """
    if not inverted_index_str or inverted_index_str in ("null", "{}"):
        return None
    try:
        inv_index = json.loads(inverted_index_str)
    except (json.JSONDecodeError, TypeError):
        return None
    if not inv_index:
        return None

    max_pos = -1
    for positions in inv_index.values():
        for p in positions:
            if p > max_pos:
                max_pos = p

    if max_pos < 0:
        return None
    if max_pos > 100_000:
        return None

    words = [""] * (max_pos + 1)
    for word, positions in inv_index.items():
        for p in positions:
            words[p] = word

    return " ".join(words)


COLUMNS = ["id", "doi", "title", "abstract_inverted_index"]

def _select_expr():
    return [
        pl.col("id").alias("work_id"),
        pl.col("doi"),
        pl.col("title"),
        _reconstruct_abstract_expr(pl.col("abstract_inverted_index"))
          .alias("abstract"),
    ]


def _process_big_file(input_path, output_dir):
    """For files >= 10 GB: read row groups in batches, write multiple output files.

    Splits into parts of BATCH_RGS row groups each to avoid memory accumulation
    from long-running ParquetWriter instances.
    """
    BATCH_RGS = 100  # ~12M rows per output file

    pf = pq.ParquetFile(input_path)
    stem = Path(input_path).stem  # e.g. "updated_date=2025-11-06"
    n_rgs = pf.metadata.num_row_groups
    part_idx = 0
    written_paths = []

    for start in range(0, n_rgs, BATCH_RGS):
        end = min(start + BATCH_RGS, n_rgs)
        tables = []
        for i in range(start, end):
            table = pf.read_row_group(i, columns=COLUMNS)
            chunk = pl.from_arrow(table)
            del table
            result = chunk.select(*_select_expr())
            del chunk
            tables.append(result.to_arrow())
            del result

        combined = pa.concat_tables(tables)
        del tables
        out_path = str(Path(output_dir) / f"{stem}.part_{part_idx:03d}.parquet")
        pq.write_table(combined, out_path)
        del combined
        gc.collect()

        written_paths.append(out_path)
        part_idx += 1

    return written_paths


def process_one_file(args):
    """Process a single parquet file using the Rust plugin."""
    input_path, output_path = args
    t0 = time.time()

    output_dir = str(Path(output_path).parent)
    out_paths = _process_big_file(input_path, output_dir)

    # Read back stats lazily from all output parts
    out_lf = pl.scan_parquet(out_paths)
    n_total = out_lf.select(pl.len()).collect().item()
    n_with_abstract = out_lf.filter(
        pl.col("abstract").is_not_null()
    ).select(pl.len()).collect().item()
    elapsed = time.time() - t0

    return {
        "file": Path(input_path).name,
        "rows": n_total,
        "with_abstract": n_with_abstract,
        "seconds": elapsed,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Reconstruct abstracts from works parquet"
    )
    parser.add_argument("--input", type=str, required=True,
                        help="Input works parquet (file or glob)")
    parser.add_argument("--output", type=str, required=True,
                        help="Output directory for parquet files")
    args = parser.parse_args()

    input_files = sorted(glob.glob(args.input))
    if not input_files:
        raise FileNotFoundError(f"No files matched: {args.input}")

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Input:   {args.input} ({len(input_files)} files)")
    print(f"Output:  {output_dir}/")
    print()

    tasks = []
    for f in input_files:
        stem = Path(f).stem
        out_path = output_dir / Path(f).name
        # Check for single file or parts from big-file processing
        part_files = sorted(output_dir.glob(f"{stem}.part_*.parquet"))

        if part_files:
            # Big file was split into parts — validate first part
            try:
                pl.scan_parquet(part_files[0]).select(pl.len()).collect()
                continue
            except Exception:
                for pf in part_files:
                    pf.unlink()
                print(f"  Removed corrupt parts: {stem}")
        elif out_path.exists():
            try:
                pl.scan_parquet(out_path).select(pl.len()).collect()
                continue
            except Exception:
                out_path.unlink()
                print(f"  Removed corrupt: {out_path.name}")

        tasks.append((f, str(out_path)))

    already_done = len(input_files) - len(tasks)
    if already_done > 0:
        print(f"Skipping {already_done} already-processed files")
        print()

    t0 = time.time()
    total_rows = 0
    total_with_abstract = 0
    completed = 0

    # Sequential processing — one file streams at a time for bounded memory
    for task in tasks:
        stats = process_one_file(task)
        completed += 1
        total_rows += stats["rows"]
        total_with_abstract += stats["with_abstract"]
        print(
            f"  [{already_done + completed}/{len(input_files)}] {stats['file']}: "
            f"{stats['rows']:,} rows, "
            f"{stats['with_abstract']:,} with abstract, "
            f"{stats['seconds']:.1f}s"
        )

    t1 = time.time()
    pct = total_with_abstract / total_rows * 100 if total_rows > 0 else 0

    total_size_mb = sum(
        f.stat().st_size for f in output_dir.glob("*.parquet")
    ) / 1024 / 1024

    print(f"\n{'─' * 60}")
    print(f"SUMMARY")
    print(f"{'─' * 60}")
    print(f"  Records:          {total_rows:,}")
    print(f"  With abstract:    {total_with_abstract:,} ({pct:.1f}%)")
    print(f"  Output:           {output_dir}/ ({len(input_files)} files)")
    print(f"  Total size:       {total_size_mb:,.1f} MB")
    print(f"  Total time:       {t1 - t0:.1f}s")


if __name__ == "__main__":
    main()
