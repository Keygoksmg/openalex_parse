"""
Schema detection utility for OpenAlex data.

Samples N records from raw gz JSON, catalogs every field with type/depth/frequency,
then diffs against a user-defined schema config.

Usage:
    # Detect and diff against user schema
    python -m openalex_parse.schema_detect \
        --data-dir /path/to/openalex/data/works \
        --schema openalex_parse/schemas/works.py

    # Just detect, no comparison
    python -m openalex_parse.schema_detect \
        --data-dir /path/to/openalex/data/authors \
        --detect-only

    # Auto-generate a schema file (samples from earliest + latest partitions)
    python -m openalex_parse.schema_detect \
        --data-dir /path/to/openalex/data/works \
        --generate openalex_parse/schemas/works.py

    # More samples
    python -m openalex_parse.schema_detect \
        --data-dir /path/to/openalex/data/works \
        --schema openalex_parse/schemas/works.py \
        --sample-size 5000
"""

import argparse
import ast
import gzip
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


def load_user_schema(schema_path):
    """Load the user-defined schema from a config file using ast.literal_eval.

    The config file must define a dict named *_SCHEMA (e.g., WORKS_SCHEMA,
    AUTHORS_SCHEMA) where keys are field names and values have a "type" key.
    No arbitrary code is executed — only literal expressions are evaluated.
    """
    schema_path = Path(schema_path)
    source = schema_path.read_text()
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id.endswith("_SCHEMA"):
                    schema_dict = ast.literal_eval(node.value)
                    return {field: config["type"] for field, config in schema_dict.items()}

    print(f"ERROR: No *_SCHEMA dict found in {schema_path}")
    sys.exit(1)


def classify_type(value):
    """Classify a Python value into a type string."""
    if value is None:
        return "null"
    elif isinstance(value, bool):
        return "bool"
    elif isinstance(value, int):
        return "int"
    elif isinstance(value, float):
        return "float"
    elif isinstance(value, str):
        return "str"
    elif isinstance(value, list):
        if len(value) == 0:
            return "list[empty]"
        elem_types = set(classify_type(e) for e in value[:5])
        if len(elem_types) == 1:
            return f"list[{elem_types.pop()}]"
        return f"list[mixed:{','.join(sorted(elem_types))}]"
    elif isinstance(value, dict):
        return "dict"
    else:
        return type(value).__name__


def sample_records(data_dir, partition, n):
    """Read n records from a partition's gz files."""
    partition_dir = data_dir / partition
    if not partition_dir.exists():
        print(f"ERROR: Partition directory not found: {partition_dir}")
        sys.exit(1)

    gz_files = sorted(partition_dir.glob("*.gz"))
    if not gz_files:
        print(f"ERROR: No .gz files in {partition_dir}")
        sys.exit(1)

    records = []
    for gz_file in gz_files:
        with gzip.open(gz_file, "rt", encoding="utf-8") as f:
            for line in f:
                records.append(json.loads(line))
                if len(records) >= n:
                    return records
    return records


def detect_schema(records):
    """Catalog every top-level field across sampled records.

    Returns dict: field_name -> {
        "types": Counter of observed types,
        "frequency": count of records where field is present,
        "example": first non-null example value (truncated),
        "null_count": count of records where field is None,
    }
    """
    schema = defaultdict(lambda: {
        "types": defaultdict(int),
        "frequency": 0,
        "null_count": 0,
        "example": None,
    })

    for rec in records:
        for key, value in rec.items():
            info = schema[key]
            info["frequency"] += 1
            t = classify_type(value)
            info["types"][t] += 1
            if value is None:
                info["null_count"] += 1
            elif info["example"] is None:
                example_str = str(value)
                if len(example_str) > 120:
                    example_str = example_str[:120] + "..."
                info["example"] = example_str

    return dict(sorted(schema.items()))


def print_report(detected, user_schema, n_records, partition, detect_only=False):
    """Print a formatted report."""
    print("=" * 80)
    print(f"OPENALEX SCHEMA DETECTION REPORT")
    print(f"Sampled {n_records} records from {partition}")
    print("=" * 80)

    # Full detected schema
    print(f"\n{'─' * 80}")
    print(f"ALL DETECTED FIELDS ({len(detected)} fields)")
    print(f"{'─' * 80}")
    print(f"{'Field':<40} {'Types':<30} {'Freq':<8} {'Nulls':<8}")
    print(f"{'─' * 40} {'─' * 30} {'─' * 8} {'─' * 8}")
    for field, info in detected.items():
        types_str = ", ".join(
            f"{t}({c})" for t, c in sorted(info["types"].items(), key=lambda x: -x[1])
        )
        if len(types_str) > 28:
            types_str = types_str[:28] + ".."
        freq = info["frequency"]
        nulls = info["null_count"]
        if detect_only:
            marker = "  "
        else:
            marker = "  " if field in user_schema else "* "
        print(f"{marker}{field:<38} {types_str:<30} {freq:<8} {nulls:<8}")

    if detect_only:
        return

    # Diff against user schema
    detected_fields = set(detected.keys())
    user_fields = set(user_schema.keys())
    in_data_not_schema = detected_fields - user_fields
    in_schema_not_data = user_fields - detected_fields
    in_both = detected_fields & user_fields

    # Fields in data but NOT in user schema
    print(f"\n{'─' * 80}")
    print(f"FIELDS IN DATA BUT NOT IN YOUR SCHEMA ({len(in_data_not_schema)} fields)")
    print(f"{'─' * 80}")
    if in_data_not_schema:
        for field in sorted(in_data_not_schema):
            info = detected[field]
            types_str = ", ".join(
                f"{t}({c})" for t, c in sorted(info["types"].items(), key=lambda x: -x[1])
            )
            example = info["example"] or ""
            print(f"  {field:<38} {types_str}")
            if example:
                print(f"    example: {example}")
    else:
        print("  (none — your schema covers all fields in the data)")

    # Fields in user schema but NOT in data
    print(f"\n{'─' * 80}")
    print(f"FIELDS IN YOUR SCHEMA BUT NOT IN DATA ({len(in_schema_not_data)} fields)")
    print(f"{'─' * 80}")
    if in_schema_not_data:
        for field in sorted(in_schema_not_data):
            print(f"  {field} (defined as {user_schema[field]})")
        print()
        print("  These fields may have been renamed or removed in this snapshot.")
        print("  Consider removing them from your schema config.")
    else:
        print("  (none — all schema fields exist in the data)")

    # Coverage summary
    print(f"\n{'─' * 80}")
    print(f"COVERAGE SUMMARY")
    print(f"{'─' * 80}")
    print(f"  Fields in data:        {len(detected)}")
    print(f"  Fields in your schema: {len(user_schema)}")
    print(f"  Covered (in both):     {len(in_both)}")
    print(f"  Missing from schema:   {len(in_data_not_schema)}")
    print(f"  Extra in schema:       {len(in_schema_not_data)}")
    coverage_pct = len(in_both) / len(detected) * 100 if detected else 0
    print(f"  Coverage:              {coverage_pct:.1f}%")

    if in_data_not_schema:
        print(f"\n  * = field not in your schema")
        print(f"  To add missing fields, edit your schema config file.")


def sample_multi_partition(data_dir, sample_size):
    """Sample records from the earliest and latest partitions for robust detection.

    Takes sample_size/2 from the latest partition and distributes the rest
    across up to 3 of the earliest partitions. This catches fields that may
    only exist in older or newer data.
    """
    partitions = sorted(
        [d.name for d in data_dir.iterdir()
         if d.is_dir() and d.name.startswith("updated_date=")]
    )
    if not partitions:
        print(f"ERROR: No partitions found in {data_dir}")
        sys.exit(1)

    # Pick partitions: latest + up to 3 earliest (deduplicated)
    selected = [partitions[-1]]
    early = partitions[:3]
    for p in early:
        if p not in selected:
            selected.append(p)

    # Distribute sample budget
    n_latest = sample_size // 2
    n_early_each = (sample_size - n_latest) // max(len(selected) - 1, 1)

    all_records = []
    for i, partition in enumerate(selected):
        n = n_latest if i == 0 else n_early_each
        records = sample_records(data_dir, partition, n)
        print(f"  {partition}: {len(records)} records")
        all_records.extend(records)

    return all_records, selected


def infer_schema_type(type_counts):
    """Infer the schema type from observed type counts.

    Returns one of: "str", "int", "float", "bool", "json"
    """
    # Remove nulls for type inference
    non_null = {t: c for t, c in type_counts.items() if t != "null"}
    if not non_null:
        return "str"  # all-null field, default to str

    types = set(non_null.keys())

    # Any list or dict → json
    if any(t.startswith("list") or t == "dict" for t in types):
        return "json"

    # Pure bool
    if types == {"bool"}:
        return "bool"

    # Pure int (no float mixing)
    if types == {"int"}:
        return "int"

    # Float or int+float mix
    if types <= {"int", "float"}:
        return "float"

    # Everything else is str
    return "str"


def generate_schema_file(detected, output_path, entity_name, partitions_sampled):
    """Generate a schema .py file from detected fields."""
    # Sanitize entity name for use as Python identifier
    entity_name = re.sub(r"[^a-zA-Z0-9_]", "_", entity_name)

    # Classify fields
    scalars = []
    nested_objects = []
    arrays = []
    special = []

    for field, info in detected.items():
        schema_type = infer_schema_type(info["types"])
        non_null_types = {t for t in info["types"] if t != "null"}

        if field == "abstract_inverted_index":
            special.append((field, "json"))
        elif schema_type == "json":
            # Distinguish nested objects from arrays
            if any(t.startswith("list") for t in non_null_types):
                arrays.append((field, "json"))
            else:
                nested_objects.append((field, "json"))
        else:
            scalars.append((field, schema_type))

    # Build schema name from entity
    clean_name = entity_name.replace("-", "_")
    schema_var = f"{clean_name.upper()}_SCHEMA"

    # Compute max field name length for alignment
    all_fields = scalars + nested_objects + arrays + special
    max_len = max(len(f) for f, _ in all_fields) if all_fields else 30

    lines = []
    lines.append('"""')
    lines.append(f"User-defined schema config for the {clean_name} table.")
    lines.append("")
    lines.append(f"Auto-generated via schema_detect from partitions:")
    for p in partitions_sampled:
        lines.append(f"  - {p}")
    lines.append("")
    lines.append("Each entry: field_name -> {\"type\": output_type}")
    lines.append('  - "str", "int", "float", "bool" -> typed scalar columns')
    lines.append('  - "json" -> stored as JSON string (nested objects and arrays)')
    lines.append('"""')
    lines.append("")
    lines.append(f"{schema_var} = {{")

    if scalars:
        lines.append("    # ── Scalar fields ────────────────────────────────────────────────────────")
        for field, ftype in scalars:
            padding = " " * (max_len - len(field))
            lines.append(f'    "{field}":{padding} {{"type": "{ftype}"}},')

    if nested_objects:
        lines.append("")
        lines.append("    # ── Nested objects (stored as JSON string) ───────────────────────────────")
        for field, ftype in nested_objects:
            padding = " " * (max_len - len(field))
            lines.append(f'    "{field}":{padding} {{"type": "{ftype}"}},')

    if arrays:
        lines.append("")
        lines.append("    # ── Arrays (stored as JSON string) ───────────────────────────────────────")
        for field, ftype in arrays:
            padding = " " * (max_len - len(field))
            lines.append(f'    "{field}":{padding} {{"type": "{ftype}"}},')

    if special:
        lines.append("")
        lines.append("    # ── Special ──────────────────────────────────────────────────────────────")
        for field, ftype in special:
            padding = " " * (max_len - len(field))
            lines.append(f'    "{field}":{padding} {{"type": "{ftype}"}},')

    lines.append("}")
    lines.append("")

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))
    print(f"\nGenerated schema: {output_path}")
    print(f"  Variable: {schema_var}")
    print(f"  Fields:   {len(all_fields)}")


PROJECT_ROOT = Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Detect OpenAlex schema and diff against user-defined config"
    )
    parser.add_argument(
        "--data-dir", type=str, required=True,
        help="Path to data directory (e.g. /path/to/openalex/data/works)",
    )
    parser.add_argument(
        "--partition", type=str, default=None,
        help="Partition to sample from (default: latest folder in data dir)",
    )
    parser.add_argument(
        "--sample-size", type=int, default=1000,
        help="Number of records to sample (default: 1000)",
    )
    parser.add_argument(
        "--schema", type=str, default=None,
        help="Path to user-defined schema config file (required unless --detect-only)",
    )
    parser.add_argument(
        "--detect-only", action="store_true",
        help="Only detect schema, skip diff against user config",
    )
    parser.add_argument(
        "--generate", type=str, default=None,
        metavar="OUTPUT_PATH",
        help="Auto-generate a schema .py file (samples earliest + latest partitions)",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)

    # ── Generate mode: sample multiple partitions, write schema file ──────
    if args.generate:
        entity_name = data_dir.name
        print(f"Generating schema for: {entity_name}")
        print(f"Data dir: {data_dir}")
        print(f"Sampling from earliest + latest partitions...\n")

        records, partitions_sampled = sample_multi_partition(
            data_dir, args.sample_size
        )
        print(f"\nTotal: {len(records)} records from {len(partitions_sampled)} partitions\n")

        print("Detecting schema...")
        detected = detect_schema(records)

        print(f"\nDetected {len(detected)} fields:")
        for field, info in detected.items():
            schema_type = infer_schema_type(info["types"])
            types_str = ", ".join(
                f"{t}({c})" for t, c in sorted(info["types"].items(), key=lambda x: -x[1])
            )
            print(f"  {field:<40} -> {schema_type:<6}  ({types_str})")

        generate_schema_file(detected, args.generate, entity_name, partitions_sampled)
        sys.exit(0)

    # ── Normal detect/diff mode ───────────────────────────────────────────
    # Auto-detect latest partition if not specified
    if args.partition is None:
        partitions = sorted(
            [d.name for d in data_dir.iterdir() if d.is_dir() and d.name.startswith("updated_date=")]
        )
        if not partitions:
            print(f"ERROR: No partitions found in {data_dir}")
            sys.exit(1)
        args.partition = partitions[-1]
        print(f"Auto-detected latest partition: {args.partition}")

    print(f"Data dir:    {data_dir}")
    print(f"Partition:   {args.partition}")
    print(f"Sample size: {args.sample_size}")
    print()

    # Sample records
    print(f"Sampling {args.sample_size} records...")
    records = sample_records(data_dir, args.partition, args.sample_size)
    print(f"Loaded {len(records)} records.\n")

    # Detect schema
    print("Detecting schema...")
    detected = detect_schema(records)

    # Load user schema (unless detect-only)
    if args.detect_only:
        user_schema = {}
    else:
        if args.schema is None:
            print("ERROR: --schema is required unless --detect-only is set")
            sys.exit(1)
        schema_path = Path(args.schema)
        print(f"Loading user schema from {schema_path}...")
        user_schema = load_user_schema(schema_path)

    print()
    print_report(detected, user_schema, len(records), args.partition, args.detect_only)
