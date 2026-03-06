"""
Produce nanochat-compatible training data from various sources.

Writes parquet shards to ~/.cache/nanochat/base_data_climbmix/ in exactly the
format the dataloader expects: parquet files with a single 'text' column,
~1024 docs per row group, named shard_XXXXX.parquet.

The LAST shard alphabetically is always used as validation. So your naming
must ensure train shards sort before the val shard.

Usage examples:

  # From a directory of .txt files
  python -m tools.make_dataset from-files ./my_texts/ --shard-prefix custom

  # From a .jsonl file (one JSON object per line, specify the text field)
  python -m tools.make_dataset from-jsonl data.jsonl --field text

  # From a list of URLs (downloads and extracts text)
  python -m tools.make_dataset from-urls urls.txt

  # Create a validation shard from existing train shards (random sample)
  python -m tools.make_dataset make-val --frac 0.02

  # Inspect what you've created
  python -m tools.make_dataset inspect
"""

import os
import json
import glob
import random
import argparse
import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = os.path.join(os.path.expanduser("~/.cache/nanochat"), "base_data_climbmix")
DOCS_PER_ROW_GROUP = 1024


def write_shard(docs: list[str], path: str):
    """Write a list of documents to a parquet shard with proper row grouping."""
    table = pa.table({"text": docs})
    writer = pq.ParquetWriter(path, table.schema)
    for start in range(0, len(docs), DOCS_PER_ROW_GROUP):
        chunk = table.slice(start, DOCS_PER_ROW_GROUP)
        writer.write_table(chunk)
    writer.close()
    print(f"Wrote {len(docs):,} docs to {path} ({os.path.getsize(path)/1024/1024:.1f}MB)")


def write_shards(docs: list[str], prefix: str, docs_per_shard: int = 86016,
                 start_index: int = 0, val_frac: float = 0.02):
    """Split docs into train shards + 1 validation shard and write them all."""
    os.makedirs(DATA_DIR, exist_ok=True)

    # Shuffle for good mixing
    random.shuffle(docs)

    # Split off validation
    val_count = max(1, int(len(docs) * val_frac))
    val_docs = docs[:val_count]
    train_docs = docs[val_count:]

    # Write train shards
    shard_idx = start_index
    for start in range(0, len(train_docs), docs_per_shard):
        chunk = train_docs[start:start + docs_per_shard]
        path = os.path.join(DATA_DIR, f"{prefix}_{shard_idx:05d}.parquet")
        write_shard(chunk, path)
        shard_idx += 1

    # Write validation shard — must sort LAST alphabetically
    # Use zzval prefix to ensure it's last, or a high shard number
    val_path = os.path.join(DATA_DIR, f"{prefix}_99999.parquet")
    write_shard(val_docs, val_path)
    print(f"\nTotal: {len(train_docs):,} train docs in {shard_idx - start_index} shards, "
          f"{len(val_docs):,} val docs")


def from_files(args):
    """Load text from a directory of .txt/.md files."""
    patterns = ["*.txt", "*.md", "*.rst"]
    files = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(args.dir, "**", pat), recursive=True))
    files.sort()

    docs = []
    for f in files:
        with open(f, "r", encoding="utf-8", errors="replace") as fh:
            text = fh.read().strip()
            if len(text) >= args.min_chars:
                docs.append(text)

    print(f"Loaded {len(docs):,} docs from {len(files)} files in {args.dir}")
    if not docs:
        print("No documents found!")
        return
    write_shards(docs, args.shard_prefix, val_frac=args.val_frac)


def from_jsonl(args):
    """Load text from a .jsonl file."""
    docs = []
    with open(args.file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            text = obj.get(args.field, "")
            if isinstance(text, str) and len(text) >= args.min_chars:
                docs.append(text)

    print(f"Loaded {len(docs):,} docs from {args.file} (field: {args.field})")
    if not docs:
        print("No documents found!")
        return
    write_shards(docs, args.shard_prefix, val_frac=args.val_frac)


def from_parquet(args):
    """Load text from existing parquet files (reformat into nanochat shards)."""
    docs = []
    for path in glob.glob(args.pattern):
        table = pq.read_table(path, columns=[args.field])
        texts = table.column(args.field).to_pylist()
        docs.extend(t for t in texts if isinstance(t, str) and len(t) >= args.min_chars)

    print(f"Loaded {len(docs):,} docs from {args.pattern} (field: {args.field})")
    if not docs:
        print("No documents found!")
        return
    write_shards(docs, args.shard_prefix, val_frac=args.val_frac)


def inspect(args):
    """Inspect existing shards in the data directory."""
    if not os.path.exists(DATA_DIR):
        print(f"No data directory at {DATA_DIR}")
        return

    files = sorted(f for f in os.listdir(DATA_DIR) if f.endswith('.parquet'))
    if not files:
        print(f"No parquet files in {DATA_DIR}")
        return

    total_docs = 0
    total_bytes = 0
    for fname in files:
        path = os.path.join(DATA_DIR, fname)
        pf = pq.ParquetFile(path)
        n_docs = sum(pf.metadata.row_group(i).num_rows for i in range(pf.num_row_groups))
        size = os.path.getsize(path)
        total_docs += n_docs
        total_bytes += size

        role = "VAL " if fname == files[-1] else "TRAIN"
        print(f"  [{role}] {fname}: {n_docs:,} docs, {size/1024/1024:.1f}MB, "
              f"{pf.num_row_groups} row groups")

    print(f"\nTotal: {total_docs:,} docs, {total_bytes/1024/1024:.1f}MB, "
          f"{len(files)} shards ({len(files)-1} train + 1 val)")

    # Sample some docs from first shard
    if args.sample > 0:
        path = os.path.join(DATA_DIR, files[0])
        pf = pq.ParquetFile(path)
        rg = pf.read_row_group(0)
        texts = rg.column('text').to_pylist()
        print(f"\nSample docs from {files[0]}:")
        for i in range(min(args.sample, len(texts))):
            preview = texts[i][:200].replace('\n', '\\n')
            print(f"  [{i}] ({len(texts[i]):,} chars) {preview}...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Produce nanochat training data")
    sub = parser.add_subparsers(dest="command")

    # from-files
    p = sub.add_parser("from-files", help="Load from directory of text files")
    p.add_argument("dir", help="Directory containing .txt/.md files")
    p.add_argument("--shard-prefix", default="custom", help="Prefix for shard filenames")
    p.add_argument("--min-chars", type=int, default=50, help="Minimum document length")
    p.add_argument("--val-frac", type=float, default=0.02, help="Fraction for validation")

    # from-jsonl
    p = sub.add_parser("from-jsonl", help="Load from JSONL file")
    p.add_argument("file", help="Path to .jsonl file")
    p.add_argument("--field", default="text", help="JSON field containing text")
    p.add_argument("--shard-prefix", default="custom", help="Prefix for shard filenames")
    p.add_argument("--min-chars", type=int, default=50, help="Minimum document length")
    p.add_argument("--val-frac", type=float, default=0.02, help="Fraction for validation")

    # from-parquet
    p = sub.add_parser("from-parquet", help="Load from existing parquet files")
    p.add_argument("pattern", help="Glob pattern for parquet files")
    p.add_argument("--field", default="text", help="Column name containing text")
    p.add_argument("--shard-prefix", default="custom", help="Prefix for shard filenames")
    p.add_argument("--min-chars", type=int, default=50, help="Minimum document length")
    p.add_argument("--val-frac", type=float, default=0.02, help="Fraction for validation")

    # inspect
    p = sub.add_parser("inspect", help="Inspect existing data shards")
    p.add_argument("--sample", type=int, default=3, help="Number of sample docs to show")

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
    elif args.command == "from-files":
        from_files(args)
    elif args.command == "from-jsonl":
        from_jsonl(args)
    elif args.command == "from-parquet":
        from_parquet(args)
    elif args.command == "inspect":
        inspect(args)
