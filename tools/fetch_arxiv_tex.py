"""
Fetch arXiv LaTeX source papers by category and produce nanochat training shards.

Downloads raw .tex source from arXiv, performs light cleaning (removes comments,
strips preamble boilerplate), and writes parquet shards ready for training.

Usage:
  # Fetch 500 plasma physics papers
  python -m tools.fetch_arxiv_tex --category physics.plasm-ph --max-papers 500

  # Fetch multiple categories
  python -m tools.fetch_arxiv_tex --category math.AP physics.class-ph cs.NA --max-papers 1000

  # All target STEM categories (default)
  python -m tools.fetch_arxiv_tex --max-papers 200

  # Resume from where you left off (tracks downloaded IDs)
  python -m tools.fetch_arxiv_tex --resume

Rate limits: arXiv allows ~1 request/3 seconds for e-print downloads.
A full run of 10k papers takes ~8 hours. Start small, verify quality, scale up.
"""

import os
import re
import io
import json
import time
import gzip
import tarfile
import argparse
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Where nanochat expects training data
DATA_DIR = os.path.join(os.path.expanduser("~/.cache/nanochat"), "base_data_climbmix")
# Where we cache raw downloads and track state
CACHE_DIR = os.path.join(os.path.expanduser("~/.cache/nanochat"), "arxiv_tex_cache")
STATE_FILE = os.path.join(CACHE_DIR, "fetch_state.json")
DOCS_PER_ROW_GROUP = 1024

# Default categories: your target domains
DEFAULT_CATEGORIES = [
    # Physics core
    "physics.plasm-ph",    # Plasma physics
    "physics.class-ph",    # Classical physics / E&M
    "physics.optics",      # Optics, RF, photonics
    "physics.acc-ph",      # Accelerator physics
    "physics.comp-ph",     # Computational physics
    "physics.gen-ph",      # General physics
    # High energy / extreme field
    "hep-ph",              # High energy physics - phenomenology
    "hep-th",              # High energy physics - theory
    "quant-ph",            # Quantum physics
    "nucl-th",             # Nuclear theory
    # EE / signals
    "eess.SP",             # Signal processing
    "eess.SY",             # Systems and control
    # Math
    "math.AP",             # Analysis of PDEs
    "math.NA",             # Numerical analysis
    "math.MP",             # Mathematical physics
    "math.DG",             # Differential geometry
    "math.FA",             # Functional analysis
    # CS
    "cs.NA",               # Numerical analysis (CS)
    "cs.CE",               # Computational engineering
    "cs.PL",               # Programming languages
]


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"downloaded_ids": [], "category_cursors": {}}


def save_state(state):
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def fetch_paper_ids(category, max_results=500, start=0):
    """Fetch paper IDs from arXiv API by category."""
    base_url = "http://export.arxiv.org/api/query"
    ids = []
    batch_size = 100

    while len(ids) < max_results:
        n = min(batch_size, max_results - len(ids))
        params = f"search_query=cat:{category}&start={start + len(ids)}&max_results={n}&sortBy=submittedDate&sortOrder=descending"
        url = f"{base_url}?{params}"

        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=30) as resp:
                    xml_data = resp.read()
                break
            except Exception as e:
                if attempt < 2:
                    time.sleep(5)
                else:
                    print(f"  Failed to fetch IDs: {e}")
                    return ids

        root = ET.fromstring(xml_data)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        entries = root.findall("atom:entry", ns)

        if not entries:
            break

        for entry in entries:
            id_url = entry.find("atom:id", ns).text
            # Extract arxiv ID from URL: http://arxiv.org/abs/2401.12345v1
            arxiv_id = id_url.split("/abs/")[-1]
            # Remove version suffix
            arxiv_id = re.sub(r"v\d+$", "", arxiv_id)
            ids.append(arxiv_id)

        time.sleep(3)  # respect rate limit

    return ids


def download_source(arxiv_id):
    """Download LaTeX source for a paper. Returns list of (filename, content) tuples."""
    url = f"https://export.arxiv.org/e-print/{arxiv_id}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "nanochat-fetcher/0.1"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
            content_type = resp.headers.get("Content-Type", "")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise
    except Exception:
        return []

    tex_files = []

    # Try to interpret as tar.gz (most common for multi-file submissions)
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            for member in tar.getmembers():
                if member.name.endswith(".tex") and member.isfile():
                    f = tar.extractfile(member)
                    if f:
                        content = f.read()
                        try:
                            text = content.decode("utf-8")
                        except UnicodeDecodeError:
                            try:
                                text = content.decode("latin-1")
                            except:
                                continue
                        tex_files.append((member.name, text))
            return tex_files
    except (tarfile.TarError, gzip.BadGzipFile):
        pass

    # Try as plain gzipped single file
    try:
        content = gzip.decompress(data)
        text = content.decode("utf-8", errors="replace")
        if "\\begin{document}" in text or "\\section" in text:
            tex_files.append(("main.tex", text))
            return tex_files
    except:
        pass

    # Try as raw tex
    try:
        text = data.decode("utf-8", errors="replace")
        if "\\begin{document}" in text or "\\section" in text:
            tex_files.append(("main.tex", text))
    except:
        pass

    return tex_files


def clean_tex(text):
    """Light cleaning of LaTeX source. Preserves the LaTeX structure."""
    # Remove comment-only lines (lines starting with %)
    lines = text.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.lstrip()
        # Keep lines that aren't pure comments
        # But preserve \% (escaped percent) and inline comments are fine
        if stripped.startswith("%") and not stripped.startswith("%%"):
            continue
        cleaned.append(line)
    text = "\n".join(cleaned)

    # Collapse runs of 3+ blank lines to 2
    text = re.sub(r"\n{4,}", "\n\n\n", text)

    # Strip trailing whitespace per line
    text = "\n".join(line.rstrip() for line in text.split("\n"))

    return text.strip()


def find_main_tex(tex_files):
    """Given multiple .tex files from a paper, find and return the main document."""
    if len(tex_files) == 1:
        return tex_files[0][1]

    # Look for the file containing \begin{document}
    for name, content in tex_files:
        if "\\begin{document}" in content:
            return content

    # Look for main.tex or paper.tex
    for name, content in tex_files:
        basename = os.path.basename(name).lower()
        if basename in ("main.tex", "paper.tex", "article.tex", "manuscript.tex"):
            return content

    # Return the longest file
    if tex_files:
        return max(tex_files, key=lambda x: len(x[1]))[1]

    return None


def write_shard(docs, path):
    """Write docs to a parquet shard with proper row grouping."""
    table = pa.table({"text": docs})
    writer = pq.ParquetWriter(path, table.schema)
    for start in range(0, len(docs), DOCS_PER_ROW_GROUP):
        chunk = table.slice(start, DOCS_PER_ROW_GROUP)
        writer.write_table(chunk)
    writer.close()
    print(f"  Wrote {len(docs):,} docs to {path} ({os.path.getsize(path)/1024/1024:.1f}MB)")


def main():
    parser = argparse.ArgumentParser(description="Fetch arXiv LaTeX source by category")
    parser.add_argument("--category", nargs="+", default=None,
                        help=f"arXiv categories (default: all target STEM categories)")
    parser.add_argument("--max-papers", type=int, default=200,
                        help="Max papers to fetch per category")
    parser.add_argument("--shard-prefix", default="arxiv_tex",
                        help="Prefix for output shard filenames")
    parser.add_argument("--min-chars", type=int, default=500,
                        help="Minimum document length after cleaning")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from previous run")
    parser.add_argument("--delay", type=float, default=3.0,
                        help="Seconds between downloads (arXiv rate limit)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Just list paper IDs, don't download")
    args = parser.parse_args()

    categories = args.category or DEFAULT_CATEGORIES
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

    state = load_state() if args.resume else {"downloaded_ids": [], "category_cursors": {}}
    downloaded_set = set(state["downloaded_ids"])

    all_docs = []
    total_fetched = 0
    total_skipped = 0

    for category in categories:
        print(f"\n{'='*60}")
        print(f"Category: {category}")
        print(f"{'='*60}")

        cursor = state["category_cursors"].get(category, 0)
        paper_ids = fetch_paper_ids(category, max_results=args.max_papers, start=cursor)
        print(f"  Found {len(paper_ids)} paper IDs")

        if args.dry_run:
            for pid in paper_ids[:5]:
                print(f"    {pid}")
            if len(paper_ids) > 5:
                print(f"    ... and {len(paper_ids) - 5} more")
            continue

        cat_docs = 0
        for i, arxiv_id in enumerate(paper_ids):
            if arxiv_id in downloaded_set:
                total_skipped += 1
                continue

            print(f"  [{i+1}/{len(paper_ids)}] {arxiv_id}...", end=" ", flush=True)

            try:
                tex_files = download_source(arxiv_id)
            except Exception as e:
                print(f"error: {e}")
                time.sleep(args.delay)
                continue

            if not tex_files:
                print("no tex")
                downloaded_set.add(arxiv_id)
                time.sleep(args.delay)
                continue

            main_tex = find_main_tex(tex_files)
            if main_tex is None:
                print("no main doc")
                downloaded_set.add(arxiv_id)
                time.sleep(args.delay)
                continue

            cleaned = clean_tex(main_tex)
            if len(cleaned) < args.min_chars:
                print(f"too short ({len(cleaned)} chars)")
                downloaded_set.add(arxiv_id)
                time.sleep(args.delay)
                continue

            all_docs.append(cleaned)
            downloaded_set.add(arxiv_id)
            cat_docs += 1
            total_fetched += 1
            print(f"ok ({len(cleaned):,} chars, {len(tex_files)} tex files)")

            time.sleep(args.delay)

            # Save state periodically
            if total_fetched % 50 == 0:
                state["downloaded_ids"] = list(downloaded_set)
                state["category_cursors"][category] = cursor + i + 1
                save_state(state)
                print(f"  [checkpoint: {total_fetched} docs saved to state]")

        state["category_cursors"][category] = cursor + len(paper_ids)
        print(f"  Category {category}: {cat_docs} docs collected")

    if args.dry_run:
        print(f"\nDry run complete. Would fetch from {len(categories)} categories.")
        return

    # Save final state
    state["downloaded_ids"] = list(downloaded_set)
    save_state(state)

    if not all_docs:
        print("\nNo documents collected!")
        return

    print(f"\n{'='*60}")
    print(f"Total: {total_fetched} new docs, {total_skipped} skipped (already downloaded)")
    print(f"Total chars: {sum(len(d) for d in all_docs):,}")

    # Write shards
    import random
    random.shuffle(all_docs)
    docs_per_shard = 86016  # match ClimbMix format

    # Find next available shard index
    existing = [f for f in os.listdir(DATA_DIR) if f.startswith(args.shard_prefix)]
    if existing:
        max_idx = max(int(re.search(r"(\d+)", f).group(1)) for f in existing if re.search(r"(\d+)", f))
        start_idx = max_idx + 1
    else:
        start_idx = 0

    for i in range(0, len(all_docs), docs_per_shard):
        chunk = all_docs[i:i + docs_per_shard]
        shard_path = os.path.join(DATA_DIR, f"{args.shard_prefix}_{start_idx:05d}.parquet")
        write_shard(chunk, shard_path)
        start_idx += 1

    print(f"\nDone! Data written to {DATA_DIR}")
    print(f"Run `python -m tools.make_dataset inspect` to verify")


if __name__ == "__main__":
    main()
