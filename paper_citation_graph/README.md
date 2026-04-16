# Paper Citation Graph Builder

This directory contains an independent Semantic Scholar citation-graph pipeline. It does not modify the existing DBLP coauthor graph pipeline.

## Design

The pipeline has three phases:

1. **Resolution**: match each local DBLP-derived seed paper to a Semantic Scholar paper.
2. **Reference Fetching**: fetch outgoing references for each matched Semantic Scholar paper.
3. **Graph Construction**: build a directed NetworkX citation graph where `u -> v` means paper `u` references paper `v`.

All intermediate artifacts are saved as JSONL files so the pipeline is reproducible and can be resumed.

## Files

| File | Purpose |
| --- | --- |
| `config.py` | Central thresholds, paths, API settings, and requested fields |
| `semantic_scholar_client.py` | Semantic Scholar API client with retries, rate-limit handling, API key support, and disk cache |
| `matcher.py` | Local DBLP paper to Semantic Scholar matching logic |
| `graph_utils.py` | Directed graph construction, export, and summary helpers |
| `paper_graph_builder.py` | Main CLI entrypoint |

## API Key

The pipeline works without a key for small runs, but an API key is strongly recommended.

Set it as:

```bash
export S2_API_KEY="your_api_key_here"
```

The client reads this environment variable automatically and sends it as the `x-api-key` header.

## Expected Input

Input can be CSV or JSONL.

Example:

```bash
data/dblp_papers.jsonl
data/dblp_papers.csv
```

Each record can contain any subset of:

| Field | Description |
| --- | --- |
| `paper_id` or `id` | Local paper ID |
| `dblp_key` or `key` | DBLP key |
| `title` | Paper title |
| `year` | Publication year |
| `authors` or `author_names` | Author list; JSON list, pipe-separated string, or semicolon-separated string |
| `doi` | DOI, if available |
| `ee`, `url`, `paper_url`, or `dblp_url` | External URL; DOI is extracted if possible |
| `venue`, `venue_name`, or `booktitle` | Venue name |
| `semantic_scholar_paper_id` or `paperId` | Existing cached Semantic Scholar paper ID |

The pipeline can work with only `title` and `year`, but DOI or existing Semantic Scholar paper IDs will improve accuracy.

The existing DBLP `papers.csv` export from the coauthor pipeline is a valid input:

```bash
data/dblp_ai_authors_2015_2025/papers.csv
```

## Matching Policy

Resolution tries:

1. Existing Semantic Scholar paper ID, if present.
2. DOI lookup, if DOI is present.
3. Title search with year filter.
4. Title search without year filter.

Candidate scoring uses:

```text
score = w_title * title_similarity + w_year * year_score + w_authors * author_overlap
```

Default weights and thresholds are stored in `config.py`.

Match statuses include:

| Status | Meaning |
| --- | --- |
| `cached_paper_id` | Existing Semantic Scholar paper ID resolved successfully |
| `exact_doi` | DOI lookup resolved successfully |
| `high_confidence_title` | Strong title/year/author match |
| `medium_confidence_title` | Acceptable title/year/author match |
| `ambiguous` | Top candidates are too close |
| `not_found` | No candidate cleared the threshold |

The matcher uses `rapidfuzz` if installed. If not, it falls back to Python's standard `difflib`.

## Run

From this directory:

```bash
python paper_graph_builder.py \
  --input ../data/dblp_ai_authors_2015_2025/papers.csv \
  --output-dir outputs/
```

From the repository root:

```bash
python paper_citation_graph/paper_graph_builder.py \
  --input data/dblp_ai_authors_2015_2025/papers.csv \
  --output-dir paper_citation_graph/outputs/
```

Using the `modal` conda environment:

```bash
conda run -n modal python paper_citation_graph/paper_graph_builder.py \
  --input data/dblp_ai_authors_2015_2025/papers.csv \
  --output-dir paper_citation_graph/outputs/
```

For a small test run:

```bash
conda run -n modal python paper_citation_graph/paper_graph_builder.py \
  --input data/dblp_ai_authors_2015_2025/papers.csv \
  --output-dir paper_citation_graph/outputs_test/ \
  --max-papers 25 \
  --max-workers 4
```

For a faster run with an API key, increase worker count carefully and keep a small global request interval:

```bash
conda run -n modal python paper_citation_graph/paper_graph_builder.py \
  --input data/dblp_ai_authors_2015_2025/papers.csv \
  --output-dir paper_citation_graph/outputs/ \
  --max-workers 8 \
  --request-interval 0.2
```

Resume a partially completed run:

```bash
python paper_citation_graph/paper_graph_builder.py \
  --input data/dblp_ai_authors_2015_2025/papers.csv \
  --output-dir paper_citation_graph/outputs/ \
  --resume
```

Skip resolution and reuse `resolved_papers.jsonl`:

```bash
python paper_citation_graph/paper_graph_builder.py \
  --input data/dblp_ai_authors_2015_2025/papers.csv \
  --output-dir paper_citation_graph/outputs/ \
  --skip-resolution
```

## CLI Options

| Option | Description |
| --- | --- |
| `--input` | Input CSV or JSONL file |
| `--output-dir` | Output directory |
| `--max-papers` | Limit number of seed papers |
| `--resume` | Reuse existing JSONL artifacts |
| `--fetch-references` | Fetch references; enabled by default |
| `--no-fetch-references` | Build only seed paper nodes |
| `--skip-resolution` | Reuse existing `resolved_papers.jsonl` |
| `--cache-dir` | Semantic Scholar response cache directory |
| `--min-match-score` | Override the match threshold |
| `--no-cache` | Disable disk cache |
| `--max-workers` | Number of concurrent request workers for resolution and reference fetching |
| `--request-interval` | Minimum seconds between request starts across all workers |
| `--quiet-progress` | Disable terminal progress bars |

`--max-workers` adds concurrency, while `--request-interval` protects the Semantic Scholar API from bursty traffic. If you receive many HTTP 429 responses, reduce `--max-workers`, increase `--request-interval`, or set `S2_API_KEY`.

The terminal progress display shows separate progress for resolution and reference fetching, including completed count, percent, rough processing rate, matched/unmatched counts, and newly written reference edges. For the cleanest progress output during long runs, use:

```bash
--log-level WARNING
```

## Outputs

The pipeline writes:

| File | Description |
| --- | --- |
| `resolved_papers.jsonl` | Accepted local-to-Semantic-Scholar mappings |
| `unmatched_papers.jsonl` | `not_found`, `ambiguous`, and failed resolution cases |
| `paper_metadata.jsonl` | Metadata for matched seed papers and referenced papers |
| `paper_references.jsonl` | One row per directed reference edge |
| `paper_graph.graphml` | Directed citation graph in GraphML format |
| `paper_graph.gpickle` | Pickled NetworkX graph |
| `summary.json` | Summary statistics |

`paper_references.jsonl` contains at least:

```text
source_paper_id
target_paper_id
source_title
target_title
source_year
target_year
```

`summary.json` includes:

```text
number_of_seed_papers
matched_papers
unmatched_papers
ambiguous_papers
number_of_nodes
number_of_edges
average_out_degree_among_seed_papers
top_cited_nodes_inside_graph
weakly_connected_components_count
in_degree_distribution
out_degree_distribution
yearly_paper_count
```

## Graph Scope

The graph includes:

1. All matched seed papers from the local DBLP-derived dataset.
2. All referenced papers pointed to by those seed papers, even if they were not in the original seed list.

Node attributes include:

```text
paper_id
title
year
venue
authors
citation_count
reference_count
is_seed_paper
source_origin
external_ids
url
```

Edge attributes include:

```text
edge_type = "references"
source = "semantic_scholar"
```

## Common Failure Modes

| Symptom | Likely Cause | Fix |
| --- | --- | --- |
| Many `not_found` records | Titles differ between DBLP and Semantic Scholar | Lower `--min-match-score` carefully or add DOI/external IDs |
| Many `ambiguous` records | Similar paper titles or missing year/authors | Add DOI, year, or author names to the input |
| HTTP 429 | Rate limit | Set `S2_API_KEY`, keep cache enabled, resume later |
| Empty references | Semantic Scholar has no references for the paper or endpoint returned none | Check individual paper metadata |
| Slow large run | Tens of thousands of API calls | Use `--resume`, keep cache enabled, and run in phases |

## Adaptation Notes

You may need to adapt field names if your local DBLP export differs. The most important fields are:

```text
title
year
doi
author_names
dblp_key
ee
```

Field normalization lives in `matcher.py`, especially `local_paper_from_record()`.

Endpoint-specific behavior is isolated in `semantic_scholar_client.py`, so if Semantic Scholar changes an endpoint or response shape, patch that file first.
