use clap::{Parser, Subcommand};
use comfy_table::{Table, ContentArrangement};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use rayon::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

fn default_data_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    let base = std::env::var("NANOCHAT_BASE_DIR")
        .unwrap_or_else(|_| format!("{home}/.cache/nanochat"));
    PathBuf::from(base).join("base_data_climbmix")
}

fn list_shards(data_dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(data_dir)
        .unwrap_or_else(|_| panic!("Cannot read {}", data_dir.display()))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect();
    files.sort();
    files
}

/// Read all documents from a parquet file
fn read_docs(path: &Path) -> Vec<String> {
    let file = fs::File::open(path).expect("Cannot open parquet file");
    let reader = SerializedFileReader::new(file).expect("Cannot read parquet");
    let mut docs = Vec::new();
    let iter = reader.get_row_iter(None).expect("Cannot iterate rows");
    for row in iter {
        let row = row.expect("Bad row");
        for (_name, field) in row.get_column_iter() {
            if let Field::Str(text) = field {
                docs.push(text.clone());
            }
        }
    }
    docs
}

#[derive(Parser)]
#[command(name = "dataview", about = "Review and analyze nanochat training data")]
struct Cli {
    /// Path to data directory (default: ~/.cache/nanochat/base_data_climbmix)
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show stats about the dataset (doc counts, lengths, etc.)
    Stats,

    /// Search for documents containing a keyword or regex pattern
    Search {
        /// Regex pattern to search for
        pattern: String,
        /// Maximum results to show
        #[arg(short = 'n', long, default_value = "10")]
        max_results: usize,
        /// Show N chars of context around each match
        #[arg(short, long, default_value = "150")]
        context: usize,
        /// Case insensitive search
        #[arg(short = 'i', long)]
        ignore_case: bool,
    },

    /// Count occurrences of a pattern across all documents
    Count {
        /// Regex pattern to count
        pattern: String,
        /// Case insensitive
        #[arg(short = 'i', long)]
        ignore_case: bool,
    },

    /// Show the top N most common words/phrases
    TopWords {
        /// Number of top words to show
        #[arg(short = 'n', long, default_value = "50")]
        top_n: usize,
        /// Minimum word length
        #[arg(long, default_value = "4")]
        min_len: usize,
    },

    /// Filter docs by length, keyword presence, etc. and report what would be kept/removed
    Filter {
        /// Minimum document length in chars
        #[arg(long)]
        min_chars: Option<usize>,
        /// Maximum document length in chars
        #[arg(long)]
        max_chars: Option<usize>,
        /// Keep only docs matching this regex
        #[arg(long)]
        must_match: Option<String>,
        /// Remove docs matching this regex
        #[arg(long)]
        must_not_match: Option<String>,
        /// Case insensitive for regex patterns
        #[arg(short = 'i', long)]
        ignore_case: bool,
        /// Actually write filtered output (default: dry run)
        #[arg(long)]
        apply: bool,
        /// Output directory for filtered shards
        #[arg(long)]
        output_dir: Option<PathBuf>,
    },

    /// Show a random sample of documents
    Sample {
        /// Number of docs to show
        #[arg(short = 'n', long, default_value = "5")]
        count: usize,
        /// Max chars to display per doc
        #[arg(long, default_value = "500")]
        max_display: usize,
    },

    /// Show a specific document by shard and index
    Show {
        /// Shard filename (e.g. shard_00000.parquet)
        shard: String,
        /// Document index within the shard
        index: usize,
    },

    /// Find duplicate or near-duplicate documents
    Dupes {
        /// Show top N most duplicated
        #[arg(short = 'n', long, default_value = "20")]
        top_n: usize,
    },
}

fn cmd_stats(data_dir: &Path) {
    let shards = list_shards(data_dir);
    if shards.is_empty() {
        println!("No shards found in {}", data_dir.display());
        return;
    }

    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table.set_header(vec!["Shard", "Role", "Docs", "Size (MB)", "Row Groups",
                          "Min Len", "Max Len", "Median Len", "Mean Len"]);

    let mut total_docs = 0usize;
    let mut total_bytes = 0u64;
    let mut all_lengths: Vec<usize> = Vec::new();

    for (i, path) in shards.iter().enumerate() {
        let role = if i == shards.len() - 1 { "VAL" } else { "TRAIN" };
        let docs = read_docs(path);
        let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let file = fs::File::open(path).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let n_rg = reader.metadata().num_row_groups();

        let mut lengths: Vec<usize> = docs.iter().map(|d| d.len()).collect();
        lengths.sort();

        let min_len = lengths.first().copied().unwrap_or(0);
        let max_len = lengths.last().copied().unwrap_or(0);
        let median = if lengths.is_empty() { 0 } else { lengths[lengths.len() / 2] };
        let mean = if lengths.is_empty() { 0 } else { lengths.iter().sum::<usize>() / lengths.len() };

        total_docs += docs.len();
        total_bytes += size;
        all_lengths.extend(lengths);

        let fname = path.file_name().unwrap().to_string_lossy();
        table.add_row(vec![
            fname.to_string(),
            role.to_string(),
            format!("{}", docs.len()),
            format!("{:.1}", size as f64 / 1024.0 / 1024.0),
            format!("{n_rg}"),
            format!("{min_len}"),
            format!("{max_len}"),
            format!("{median}"),
            format!("{mean}"),
        ]);
    }

    println!("{table}");
    all_lengths.sort();
    let total_chars: usize = all_lengths.iter().sum();
    println!("\nTotal: {total_docs} docs, {:.1}MB, {:.1}M chars",
             total_bytes as f64 / 1024.0 / 1024.0,
             total_chars as f64 / 1_000_000.0);

    // Length distribution buckets
    let buckets = [100, 500, 1000, 2000, 5000, 10000, 50000, 100000];
    println!("\nLength distribution:");
    let mut prev = 0;
    for &b in &buckets {
        let count = all_lengths.iter().filter(|&&l| l >= prev && l < b).count();
        let pct = 100.0 * count as f64 / all_lengths.len() as f64;
        println!("  {prev:>6} - {b:<6}: {count:>6} ({pct:>5.1}%)");
        prev = b;
    }
    let count = all_lengths.iter().filter(|&&l| l >= prev).count();
    let pct = 100.0 * count as f64 / all_lengths.len() as f64;
    println!("  {prev:>6}+       : {count:>6} ({pct:>5.1}%)");
}

fn cmd_search(data_dir: &Path, pattern: &str, max_results: usize, context: usize, ignore_case: bool) {
    let re = regex::RegexBuilder::new(pattern)
        .case_insensitive(ignore_case)
        .build()
        .unwrap_or_else(|e| panic!("Invalid regex: {e}"));

    let shards = list_shards(data_dir);
    let found = AtomicUsize::new(0);
    let total_matches = AtomicUsize::new(0);
    let results: Mutex<Vec<(String, usize, String)>> = Mutex::new(Vec::new());

    shards.par_iter().for_each(|path| {
        let fname = path.file_name().unwrap().to_string_lossy().to_string();
        let docs = read_docs(path);
        for (doc_idx, doc) in docs.iter().enumerate() {
            if found.load(Ordering::Relaxed) >= max_results {
                return;
            }
            let matches: Vec<_> = re.find_iter(doc).collect();
            if matches.is_empty() {
                continue;
            }
            total_matches.fetch_add(matches.len(), Ordering::Relaxed);
            if found.fetch_add(1, Ordering::Relaxed) >= max_results {
                return;
            }
            // Extract context around first match
            let m = &matches[0];
            let start = m.start().saturating_sub(context);
            let end = (m.end() + context).min(doc.len());
            // Align to char boundaries
            let start = doc.floor_char_boundary(start);
            let end = doc.ceil_char_boundary(end);
            let snippet = &doc[start..end];
            let snippet = snippet.replace('\n', "\\n");
            let info = format!(
                "[{fname} doc {doc_idx}] ({} chars, {} matches) ...{}...",
                doc.len(), matches.len(), snippet
            );
            results.lock().unwrap().push((fname.clone(), doc_idx, info));
        }
    });

    let results = results.into_inner().unwrap();
    for (_, _, info) in &results {
        println!("{info}");
        println!();
    }
    println!("Found {} docs with matches ({} total occurrences)",
             results.len(), total_matches.load(Ordering::Relaxed));
}

fn cmd_count(data_dir: &Path, pattern: &str, ignore_case: bool) {
    let re = regex::RegexBuilder::new(pattern)
        .case_insensitive(ignore_case)
        .build()
        .unwrap_or_else(|e| panic!("Invalid regex: {e}"));

    let shards = list_shards(data_dir);
    let total_matches = AtomicUsize::new(0);
    let total_docs_with_match = AtomicUsize::new(0);
    let total_docs = AtomicUsize::new(0);

    shards.par_iter().for_each(|path| {
        let docs = read_docs(path);
        total_docs.fetch_add(docs.len(), Ordering::Relaxed);
        for doc in &docs {
            let count = re.find_iter(doc).count();
            if count > 0 {
                total_matches.fetch_add(count, Ordering::Relaxed);
                total_docs_with_match.fetch_add(1, Ordering::Relaxed);
            }
        }
    });

    let n_docs = total_docs.load(Ordering::Relaxed);
    let n_matches = total_matches.load(Ordering::Relaxed);
    let n_docs_match = total_docs_with_match.load(Ordering::Relaxed);
    let pct = if n_docs > 0 { 100.0 * n_docs_match as f64 / n_docs as f64 } else { 0.0 };
    println!("Pattern: {pattern}");
    println!("Total occurrences: {n_matches}");
    println!("Docs with matches: {n_docs_match}/{n_docs} ({pct:.1}%)");
}

fn cmd_top_words(data_dir: &Path, top_n: usize, min_len: usize) {
    let shards = list_shards(data_dir);
    let word_re = Regex::new(r"\b[a-zA-Z]+\b").unwrap();

    let per_shard: Vec<HashMap<String, usize>> = shards.par_iter().map(|path| {
        let docs = read_docs(path);
        let mut counts: HashMap<String, usize> = HashMap::new();
        for doc in &docs {
            for m in word_re.find_iter(doc) {
                let word = m.as_str().to_lowercase();
                if word.len() >= min_len {
                    *counts.entry(word).or_insert(0) += 1;
                }
            }
        }
        counts
    }).collect();

    // Merge
    let mut merged: HashMap<String, usize> = HashMap::new();
    for shard_counts in per_shard {
        for (word, count) in shard_counts {
            *merged.entry(word).or_insert(0) += count;
        }
    }

    let mut sorted: Vec<_> = merged.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));

    let mut table = Table::new();
    table.set_header(vec!["Rank", "Word", "Count"]);
    for (i, (word, count)) in sorted.iter().take(top_n).enumerate() {
        table.add_row(vec![
            format!("{}", i + 1),
            word.clone(),
            format!("{count}"),
        ]);
    }
    println!("{table}");
}

fn cmd_filter(
    data_dir: &Path,
    min_chars: Option<usize>,
    max_chars: Option<usize>,
    must_match: Option<&str>,
    must_not_match: Option<&str>,
    ignore_case: bool,
    apply: bool,
    output_dir: Option<&Path>,
) {
    let must_re = must_match.map(|p| {
        regex::RegexBuilder::new(p).case_insensitive(ignore_case).build()
            .unwrap_or_else(|e| panic!("Invalid must_match regex: {e}"))
    });
    let must_not_re = must_not_match.map(|p| {
        regex::RegexBuilder::new(p).case_insensitive(ignore_case).build()
            .unwrap_or_else(|e| panic!("Invalid must_not_match regex: {e}"))
    });

    let shards = list_shards(data_dir);
    let mut total_kept = 0usize;
    let mut total_removed = 0usize;
    let mut removal_reasons: HashMap<String, usize> = HashMap::new();

    for path in &shards {
        let fname = path.file_name().unwrap().to_string_lossy();
        let docs = read_docs(path);
        let mut kept = Vec::new();
        let mut removed = 0usize;

        for doc in &docs {
            let mut dominated = false;
            let mut reason = String::new();

            if let Some(min) = min_chars {
                if doc.len() < min {
                    dominated = true;
                    reason = format!("too_short (<{min})");
                }
            }
            if !dominated {
                if let Some(max) = max_chars {
                    if doc.len() > max {
                        dominated = true;
                        reason = format!("too_long (>{max})");
                    }
                }
            }
            if !dominated {
                if let Some(ref re) = must_re {
                    if !re.is_match(doc) {
                        dominated = true;
                        reason = "no_required_match".into();
                    }
                }
            }
            if !dominated {
                if let Some(ref re) = must_not_re {
                    if re.is_match(doc) {
                        dominated = true;
                        reason = "excluded_pattern".into();
                    }
                }
            }

            if dominated {
                removed += 1;
                *removal_reasons.entry(reason).or_insert(0) += 1;
            } else {
                kept.push(doc.clone());
            }
        }

        println!("{fname}: {}/{} kept ({removed} removed)",
                 kept.len(), docs.len());
        total_kept += kept.len();
        total_removed += removed;

        if apply {
            let out_dir = output_dir.unwrap_or(data_dir);
            std::fs::create_dir_all(out_dir).ok();
            let out_path = out_dir.join(path.file_name().unwrap());
            write_parquet_shard(&out_path, &kept);
        }
    }

    println!("\nSummary: {total_kept} kept, {total_removed} removed");
    if !removal_reasons.is_empty() {
        println!("Removal reasons:");
        let mut sorted: Vec<_> = removal_reasons.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));
        for (reason, count) in sorted {
            println!("  {reason}: {count}");
        }
    }
    if !apply && total_removed > 0 {
        println!("\n(Dry run — use --apply to write filtered output)");
    }
}

fn write_parquet_shard(path: &Path, docs: &[String]) {
    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let schema = Schema::new(vec![Field::new("text", DataType::Utf8, true)]);
    let schema = std::sync::Arc::new(schema);

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(1024)
        .build();

    let file = fs::File::create(path).expect("Cannot create output file");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .expect("Cannot create arrow writer");

    for chunk in docs.chunks(1024) {
        let array = StringArray::from(chunk.to_vec());
        let batch = RecordBatch::try_new(schema.clone(), vec![std::sync::Arc::new(array)])
            .expect("Cannot create batch");
        writer.write(&batch).expect("Cannot write batch");
    }
    writer.close().expect("Cannot close writer");
    println!("  Wrote {} docs to {}", docs.len(), path.display());
}

fn cmd_sample(data_dir: &Path, count: usize, max_display: usize) {
    let shards = list_shards(data_dir);
    if shards.is_empty() {
        println!("No shards found.");
        return;
    }

    // Collect all docs with their shard info
    let shard = &shards[0]; // sample from first shard
    let docs = read_docs(shard);
    let fname = shard.file_name().unwrap().to_string_lossy();

    use std::collections::HashSet;
    let mut rng_state = 42u64;
    let mut shown = HashSet::new();
    for _ in 0..count {
        // Simple LCG random
        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = (rng_state >> 33) as usize % docs.len();
        if !shown.insert(idx) { continue; }
        let doc = &docs[idx];
        let display = if doc.len() > max_display { &doc[..doc.floor_char_boundary(max_display)] } else { doc };
        let display = display.replace('\n', "\n  | ");
        println!("--- [{fname} doc {idx}] ({} chars) ---", doc.len());
        println!("  | {display}");
        if doc.len() > max_display {
            println!("  | ... ({} more chars)", doc.len() - max_display);
        }
        println!();
    }
}

fn cmd_show(data_dir: &Path, shard_name: &str, index: usize) {
    let path = data_dir.join(shard_name);
    if !path.exists() {
        println!("Shard not found: {}", path.display());
        return;
    }
    let docs = read_docs(&path);
    if index >= docs.len() {
        println!("Index {index} out of range (shard has {} docs)", docs.len());
        return;
    }
    println!("--- [{shard_name} doc {index}] ({} chars) ---\n", docs[index].len());
    println!("{}", docs[index]);
}

fn cmd_dupes(data_dir: &Path, top_n: usize) {
    let shards = list_shards(data_dir);
    let mut hash_counts: HashMap<u64, (usize, String, String)> = HashMap::new();

    for path in &shards {
        let fname = path.file_name().unwrap().to_string_lossy().to_string();
        let docs = read_docs(path);
        for (i, doc) in docs.iter().enumerate() {
            // Simple hash: first 200 chars
            let key_str = if doc.len() > 200 { &doc[..doc.floor_char_boundary(200)] } else { doc.as_str() };
            let hash = {
                let mut h = 0xcbf29ce484222325u64;
                for b in key_str.bytes() {
                    h ^= b as u64;
                    h = h.wrapping_mul(0x100000001b3);
                }
                h
            };
            hash_counts.entry(hash)
                .and_modify(|(count, _, _)| *count += 1)
                .or_insert((1, fname.clone(), format!("doc {i}: {}",
                    doc.chars().take(100).collect::<String>().replace('\n', "\\n"))));
        }
    }

    let mut dupes: Vec<_> = hash_counts.into_iter()
        .filter(|(_, (count, _, _))| *count > 1)
        .collect();
    dupes.sort_by(|a, b| b.1.0.cmp(&a.1.0));

    if dupes.is_empty() {
        println!("No duplicates found!");
        return;
    }

    println!("Top duplicated documents (by first 200 chars):\n");
    for (_, (count, shard, preview)) in dupes.iter().take(top_n) {
        println!("  {count}x [{shard}] {preview}");
    }
    let total_dupes: usize = dupes.iter().map(|(_, (c, _, _))| c - 1).sum();
    println!("\n{} duplicate groups, {total_dupes} redundant copies", dupes.len());
}

fn main() {
    let cli = Cli::parse();
    let data_dir = cli.data_dir.unwrap_or_else(default_data_dir);

    match cli.command {
        Commands::Stats => cmd_stats(&data_dir),
        Commands::Search { pattern, max_results, context, ignore_case } =>
            cmd_search(&data_dir, &pattern, max_results, context, ignore_case),
        Commands::Count { pattern, ignore_case } =>
            cmd_count(&data_dir, &pattern, ignore_case),
        Commands::TopWords { top_n, min_len } =>
            cmd_top_words(&data_dir, top_n, min_len),
        Commands::Filter { min_chars, max_chars, must_match, must_not_match, ignore_case, apply, output_dir } =>
            cmd_filter(&data_dir, min_chars, max_chars,
                       must_match.as_deref(), must_not_match.as_deref(),
                       ignore_case, apply, output_dir.as_deref()),
        Commands::Sample { count, max_display } =>
            cmd_sample(&data_dir, count, max_display),
        Commands::Show { shard, index } =>
            cmd_show(&data_dir, &shard, index),
        Commands::Dupes { top_n } =>
            cmd_dupes(&data_dir, top_n),
    }
}
