# Bridging Research Communities: A Network Analysis of Scientific Collaboration
This project explores community structure and bridge authors in scientific collaboration networks using graph analysis methods. More specificly, We construct a co-authorship network to study collaboration structure, and use paper abstracts and citation information as auxiliary signals to interpret community topics and measure cross-community influence.

## Table of Contents

- [Project Motivation](#project-motivation)
- [Research Questions](#research-questions)
- [Expected Findings](#expected-findings)
- [Dataset](#dataset)
  - [Primary Dataset Option: DBLP](#primary-dataset-option-dblp)
  - [Why DBLP?](#why-dblp)
  - [Possible Alternative Datasets](#possible-alternative-datasets)
- [Network Construction](#network-construction)
- [Next-step: Potential Collaborator Recommendation](#next-step-potential-collaborator-recommendation)
- [DBLP AI Author Graph](#dblp-ai-author-graph)
  - [The data we can fetch from dblp](#the-data-we-can-fetch-from-dblp)
- [Internal Paper Citation Graph](#internal-paper-citation-graph)
- [Current Pipeline](#current-pipeline)
  - [1. DBLP Data Collection Pipeline](#1-dblp-data-collection-pipeline)
  - [2. Author Affiliation Enrichment Pipelines](#2-author-affiliation-enrichment-pipelines)
  - [3. Coauthor Graph Visualization Pipeline](#3-coauthor-graph-visualization-pipeline)
  - [4. Affiliation Treemap Pipeline](#4-affiliation-treemap-pipeline)
  - [5. Interactive Plotly Treemap Pipeline](#5-interactive-plotly-treemap-pipeline)
  - [6. Bridge Author Visualization Pipeline](#6-bridge-author-visualization-pipeline)

## Project Motivation
Scientific collaboration is not uniformly distributed across researchers. Instead, it tends to form communities of tightly connected authors, where most collaborations happen within the same group. At the same time, a small number of authors connect otherwise separated communities and play an important role in enabling cross-community knowledge exchange.

This project studies the structure of scientific collaboration networks and focuses on two main questions: how research communities emerge, and which authors act as bridges between them. By modeling academic collaboration as a graph, the project aims to better understand how research is socially organized and how different communities are connected.

## Research Questions

This project is built around the following questions:

1. **What community structure exists in the scientific collaboration network?**
   Are there clearly identifiable groups of authors corresponding to research communities?

2. **Who are the bridge authors?**
   Which authors connect multiple communities and facilitate cross-community collaboration?

3. **How concentrated is scientific collaboration?**
   Is the network dominated by a small number of highly connected authors, or is collaboration more evenly distributed?

4. **Do collaboration communities align with semantic research topics?**
   Do the papers published by the same community have the same focus, and what are they?

## Expected Findings

We expect the collaboration network to exhibit a strong community structure, where most authors belong to relatively dense local groups. We also expect that:

* most collaborations will occur **within communities** rather than across them;
* a small number of authors will have disproportionately high **bridge importance**;
* highly productive authors will not always be the same as the most important **bridge authors**;
* some communities will appear relatively **isolated**, while others will be more open and interconnected.

These findings would help illustrate how scientific research is organized as a network of communities, with a limited number of connectors enabling broader collaboration.

## Dataset

This project uses an academic bibliographic dataset to construct a **co-authorship network**.

### Primary Dataset Option: DBLP

**DBLP** is a large open bibliographic database for computer science publications. It provides structured metadata for academic papers, including:

* paper titles
* author names
* publication years
* venues (conference or journal)
* electronic edition links and, in some cases, DOI information

In this project, DBLP is used to build a **co-authorship graph**, where:

* each **node** represents an author;
* an **edge** is created between two authors if they have co-authored at least one paper;
* edge weights may optionally represent the number of collaborations between two authors.

DBLP is particularly suitable for this project because it offers clean and structured metadata for computer science research, making it well suited for collaboration network analysis.

### Why DBLP?

DBLP is chosen because it is:

* well structured and widely used in academic network studies;
* highly relevant for computer science collaboration analysis;
* convenient for constructing author-level graphs;
* appropriate for tasks such as community detection, centrality analysis, and bridge author discovery.

### Possible Alternative Datasets

Depending on the scope of the project, other datasets could also be considered:

* **OpenAlex**: broader coverage across disciplines, suitable for large-scale academic graph analysis;
* **Semantic Scholar**: useful for paper-level and citation-based analysis;
* **arXiv metadata**: useful for topic and text-based analysis, especially when focusing on abstracts and categories.

However, for a project centered on **co-authorship networks and bridge authors**, DBLP is the most straightforward and practical choice.

## Network Construction

The co-authorship network is constructed as follows:

1. Extract publications and author lists from the dataset.
2. For each paper, connect every pair of co-authors.
3. Build an undirected graph where:

   * nodes represent authors,
   * edges represent collaboration relationships.
4. Optionally assign edge weights based on the number of joint publications.

This graph serves as the foundation for all subsequent analyses, including community detection, centrality analysis, and bridge author identification.

## Next-step: Potential Collaborator Recommendation

In addition to structural analysis of the co-authorship network, this project can be extended to a **potential collaborator recommendation** task.

Given a target author, the goal is to recommend other authors who are likely to become valuable future collaborators. This task can be formulated as a **link prediction problem** on the co-authorship graph, where:

- each **node** represents an author;
- an **edge** indicates an existing collaboration;
- missing edges represent potential future collaborations.

### Motivation
Scientific collaboration networks do not only reveal existing research communities; they can also be used to identify promising future connections. Recommending potential collaborators may help uncover:

- likely future co-authors based on current network structure;
- researchers with similar interests who have not yet collaborated;
- cross-community collaboration opportunities that may connect otherwise separated research groups.

### Recommendation Objective
For a given target author, the system aims to rank candidate collaborators based on a combination of:

- **network proximity**: how close two authors are in the collaboration graph;
- **semantic similarity**: how similar their research topics are based on paper abstracts or keywords;
- **cross-community potential**: whether a collaboration could connect different research communities.

## DBLP AI Author Graph

If you want to build the DBLP coauthor graph under the definition:

`AI author = an author who published at least one paper in AAAI, IJCAI, ICML, NeurIPS, or ICLR during a chosen year range`

the main entrypoint is:

```bash
python scripts/fetch_dblp_ai_coauthor_graph.py --start-year 2015 --end-year 2025
```

The script writes:

- `data/dblp_ai_authors_<start>_<end>/authors.csv`
- `data/dblp_ai_authors_<start>_<end>/edges.csv`
- `data/dblp_ai_authors_<start>_<end>/papers.csv`
- `data/dblp_ai_authors_<start>_<end>/graph.graphml`
- `data/dblp_ai_authors_<start>_<end>/summary.json`

The constructed graph is an undirected weighted coauthor graph where edge weight equals the number of qualifying papers coauthored inside this venue/year slice.

The script works by:

- fetching each proceedings record from `https://dblp.org/rec/conf/<venue>/<year>.xml`;
- reading the per-year table-of-contents path from the record;
- converting that path to a TOC XML endpoint under `https://dblp.org/db/conf/...`;
- extracting paper-level `<inproceedings>` entries and their authors.

Example: fetch only `2015-2025` for the five flagship venues:

```bash
python scripts/fetch_dblp_ai_coauthor_graph.py --start-year 2015 --end-year 2025
```

Example: fetch only `ICML` and `NeurIPS`:

```bash
python scripts/fetch_dblp_ai_coauthor_graph.py --venues icml nips --start-year 2015 --end-year 2025
```

Because DBLP rate limits frequent requests, the script includes a polite delay and
automatic retry logic for `429 Too Many Requests`.

### The data we can fetch from dblp
`rec/conf/<venue>/<year>.xml`：proceedings record
example: https://dblp.org/rec/conf/iclr/2025.xml
the structures are like:
```xml
<dblp>
  <proceedings key="conf/iclr/2025" mdate="2025-05-12">
    <title>The Thirteenth International Conference on Learning Representations, ICLR 2025, Singapore, April 24-28, 2025</title>
    <booktitle>ICLR</booktitle>
    <publisher>OpenReview.net</publisher>
    <year>2025</year>
    <ee type="oa">https://openreview.net/group?id=ICLR.cc/2025/Conference</ee>
    <url>db/conf/iclr/iclr2025.html</url>
  </proceedings>
</dblp>
```

`db/conf/.../<venue><year>.xml`：TOC / paper list
The list of all the papers in one conference 

Example: `https://dblp.org/db/conf/iclr/iclr2025.xml`

The structure:
```XML
<bht key="db/conf/iclr/iclr2025.bht" title="ICLR 2025">
  <h1>13th ICLR 2025: Singapore</h1>

  <dblpcites>
    <r>
      <proceedings key="conf/iclr/2025">
        ...
      </proceedings>
    </r>
  </dblpcites>

  <h2>Accept (Oral)</h2>

  <dblpcites>
    <r style="ee">
      <inproceedings key="conf/iclr/KranNKJPJ25" mdate="2025-06-13">
        <author pid="345/8444">Esben Kran</author>
        <author pid="407/2861">Jord Nguyen</author>
        <title>DarkBench: Benchmarking Dark Patterns in Large Language Models.</title>
        <year>2025</year>
        <booktitle>ICLR</booktitle>
        <ee type="oa">https://openreview.net/forum?id=...</ee>
        <crossref>conf/iclr/2025</crossref>
        <url>db/conf/iclr/iclr2025.html#KranNKJPJ25</url>
      </inproceedings>
    </r>
  </dblpcites>
</bht>

```

Each `<inproceedings>` record usually contains:

| Field | Description |
| --- | --- |
| key | DBLP paper key, for example `conf/iclr/KranNKJPJ25` |
| mdate | Last modification date of this DBLP record |
| author | Author; the `pid` attribute is the DBLP author ID |
| title | Paper title |
| year | Publication year |
| booktitle | Conference abbreviation |
| ee | External electronic edition link, such as OpenReview / PMLR / DOI |
| crossref | Reference to the proceedings key |
| url | DBLP page anchor |
| pages | Page numbers, missing for some conferences |

## Internal Paper Citation Graph

If you want to build a citation graph only among the papers already listed in `papers.csv`,
you can run:

```bash
python paper_citation_graph/paper_graph_builder.py \
  --input data/dblp_ai_authors_2015_2025/papers.csv \
  --output-dir paper_citation_graph/outputs
```

This pipeline works in three stages:

1. Resolve each local DBLP paper to a Semantic Scholar `paperId` using DOI first, then title/year/author matching.
2. Fetch the outgoing references for each matched paper from Semantic Scholar.
3. Keep only edges where both source and target papers are already present in your local `papers.csv`.

Main outputs:

- `paper_citation_graph/outputs/resolved_papers.jsonl`
- `paper_citation_graph/outputs/unmatched_papers.jsonl`
- `paper_citation_graph/outputs/paper_references.jsonl`
- `paper_citation_graph/outputs/internal_citation_edges.csv`
- `paper_citation_graph/outputs/paper_graph.graphml`
- `paper_citation_graph/outputs/summary.json`

Useful options:

- `--max-papers 200`: test on a smaller subset first
- `--resume`: continue from existing resolution/reference artifacts
- `--skip-resolution`: reuse `resolved_papers.jsonl` and fetch references only
- `--request-interval 1.5`: slow requests down if you see throttling
- `--no-cache`: disable local API response cache

If you have a Semantic Scholar API key, set `S2_API_KEY` first for more stable throughput.

## Current Pipeline

### 1. DBLP Data Collection Pipeline

The DBLP data collection pipeline is implemented in:

```bash
scripts/fetch_dblp_ai_coauthor_graph.py
```

Its goal is to build a co-authorship graph for authors who published in the selected AI venues during the selected year range.

The default venue set is:

```text
AAAI, IJCAI, ICML, NeurIPS, ICLR
```

In DBLP venue keys, NeurIPS is represented as `nips`.

The pipeline works as follows:

1. Read the selected venue keys and year range.
2. For each venue-year pair, request the DBLP proceedings record:

```text
https://dblp.org/rec/conf/<venue>/<year>.xml
```

3. Extract the DBLP table-of-contents path from the `<url>` field in the proceedings record.
4. Convert the TOC path from `.html` to `.xml`.
5. Request the DBLP TOC XML file:

```text
https://dblp.org/db/conf/.../<venue><year>.xml
```

6. Iterate over every `<inproceedings>` entry in the TOC XML.
7. Keep only entries whose `<year>` matches the requested year.
8. Extract paper metadata, including title, venue, DBLP key, external link, and author list.
9. Use DBLP `pid` values as stable author IDs when available.
10. Build an undirected weighted coauthor graph:

```text
node = author
edge = two authors coauthored at least one qualifying paper
edge weight = number of qualifying papers coauthored by the two authors
```

The pipeline exports the following files:

| File | Description |
| --- | --- |
| `papers.csv` | Paper-level metadata extracted from DBLP |
| `authors.csv` | Author-level metadata and publication counts |
| `edges.csv` | Weighted coauthor edges |
| `graph.graphml` | GraphML version of the coauthor graph |
| `summary.json` | Dataset summary and venue/year counts |

The paper-level fields exported in `papers.csv` are:

| Field | Description |
| --- | --- |
| `paper_id` | Locally generated paper ID |
| `dblp_key` | Original DBLP paper key |
| `title` | Paper title |
| `year` | Publication year |
| `venue_key` | DBLP venue key, such as `nips`, `icml`, or `aaai` |
| `venue_name` | Display name of the venue, such as `NeurIPS` or `ICML` |
| `booktitle` | Conference or proceedings abbreviation |
| `ee` | External electronic edition link |
| `pages` | Page numbers |
| `crossref` | Reference to the proceedings key |
| `dblp_url` | DBLP page URL |
| `toc_url` | DBLP table-of-contents XML URL where the paper was found |
| `author_ids` | List of author IDs |
| `author_names` | List of author names |
| `author_count` | Number of authors |

Example command:

```bash
python scripts/fetch_dblp_ai_coauthor_graph.py --start-year 2015 --end-year 2025
```

Using the `modal` conda environment:

```bash
conda run -n modal python scripts/fetch_dblp_ai_coauthor_graph.py --start-year 2015 --end-year 2025
```

### 2. Author Affiliation Enrichment Pipelines

The repository now maintains four related author tables for the DBLP AI dataset:

| File | Description |
| --- | --- |
| `authors.csv` | Raw author table extracted directly from DBLP paper metadata |
| `authors_orcid_backfilled.csv` | Full author table after backfilling ORCID from the local DBLP person dump |
| `authors_orcid_subgraph.csv` | Filtered ORCID + affiliation subgraph author table used for community analysis |
| `authors_orcid_fullgraph.csv` | Full author table with affiliation metadata for all authors that can be enriched |

The ORCID + affiliation subgraph is produced by:

```bash
scripts/generate_orcid_subgraph_communities.py
```

This pipeline works as follows:

1. Read `authors.csv` and `edges.csv`.
2. Backfill ORCID identifiers from the local DBLP XML dump.
3. Query OpenAlex for author-level affiliation metadata.
4. Use the affiliation-country lookup table to fill missing country codes when possible.
5. Keep only authors with ORCID and non-empty affiliation.
6. Keep only edges among the retained authors.
7. Remove isolates and detect Louvain communities on the resulting subgraph.

Main outputs:

- `data/dblp_ai_authors_<start>_<end>/authors_orcid_backfilled.csv`
- `data/dblp_ai_authors_<start>_<end>/authors_orcid_subgraph.csv`
- `data/dblp_ai_authors_<start>_<end>/edges_orcid_subgraph.csv`
- `data/dblp_ai_authors_<start>_<end>/community_assignments_orcid_subgraph.csv`

If you want affiliation metadata for the full author table instead of only the filtered subgraph, use:

```bash
python scripts/generate_authors_orcid_fullgraph.py \
  --input-dir data/dblp_ai_authors_2015_2025
```

This script starts from `authors_orcid_backfilled.csv`, reuses `authors_orcid_subgraph.csv` as a warm-start cache, queries OpenAlex for remaining ORCID authors, computes `weighted_degree` from `edges.csv`, and writes:

- `data/dblp_ai_authors_<start>_<end>/authors_orcid_fullgraph.csv`
- `data/dblp_ai_authors_<start>_<end>/author_affiliation_cache_fullgraph.csv`

### 3. Coauthor Graph Visualization Pipeline

The current coauthor graph visualization pipeline is implemented in:

```bash
scripts/visualize_coauthor_graph.py
```

It now visualizes the **current ORCID + affiliation subgraph**, not the original full `authors.csv / edges.csv` graph directly.

The visualization pipeline uses:

```text
authors_orcid_subgraph.csv
edges_orcid_subgraph.csv
community_assignments_orcid_subgraph.csv
```

from:

```text
data/dblp_ai_authors_2015_2025
```

The pipeline works as follows:

1. Read `authors_orcid_subgraph.csv` and `edges_orcid_subgraph.csv`.
2. Build a NetworkX undirected weighted graph.
3. Remove edges with weight lower than `--min-edge-weight`.
4. Remove isolated nodes after edge filtering.
5. Load precomputed Louvain communities from `community_assignments_orcid_subgraph.csv` when available; otherwise recompute them.
6. Optionally remove communities whose size is smaller than `--min-community-count`.
7. If `--plot-full-graph` is not set:
   - compute weighted degree on the filtered graph
   - select the top-k authors by:

```text
weighted_degree descending
paper_count descending
name ascending
```

   - build the induced display subgraph on those top-k authors
   - remove new isolates
   - keep only the largest connected component for display
8. If `--plot-full-graph` is set:
   - visualize the full filtered ORCID + affiliation subgraph
9. Draw the graph:

```text
node = author
node size = weighted degree
node color = Louvain community
edge width = coauthorship weight
label = top weighted-degree authors
```

10. Save the final PNG image.

Default behavior:

```text
--top-k 120
--label-top-k 10
--min-edge-weight 3
--min-community-count 1
```

Default output paths:

```text
<input-dir>/orcid_subgraph_top120.png
<input-dir>/orcid_subgraph_full.png   (when --plot-full-graph is used)
```

Example: top-k static visualization

```bash
python scripts/visualize_coauthor_graph.py --input-dir data/dblp_ai_authors_2015_2025
```

Example: full-graph static visualization with small-community filtering

```bash
python scripts/visualize_coauthor_graph.py \
  --input-dir data/dblp_ai_authors_2015_2025 \
  --plot-full-graph \
  --min-edge-weight 3 \
  --min-community-count 5
```

This visualization is intended as a high-level overview of the ORCID + affiliation subgraph and its Louvain communities. Bridge-author analysis is handled separately by:

```bash
scripts/visualize_bridge_authors.py
```

### 4. Affiliation Treemap Pipeline

The repository also includes a country-grouped affiliation treemap generator:

```bash
scripts/generate_affiliation_treemap.py
```

This pipeline joins:

- `papers.csv`
- an author-affiliation table such as `authors_orcid_subgraph.csv` or `authors_orcid_fullgraph.csv`
- `affiliation_country_lookup.csv`

and produces a country-grouped treemap where each rectangle represents an affiliation and its counted paper volume.

Important note: the current treemap pipeline uses author-level affiliation metadata projected onto papers, not paper-level affiliation metadata extracted from each paper record. In other words, it maps `author_id -> affiliation` from the enriched author table and then counts papers through those author affiliations. The resulting treemap should therefore be interpreted as an author-profile-affiliation view of the paper set, rather than a strict publication-time affiliation view.

Key options:

- `--authors-csv`: choose `authors_orcid_subgraph.csv` or `authors_orcid_fullgraph.csv`
- `--author-scope all|first`: count all authors on a paper or only the first listed author
- `--count-mode paper|authorship`: count each paper once per affiliation or count every author-paper affiliation incidence

Default outputs are written to:

```text
data/dblp_ai_authors_<start>_<end>/results/
```

Typical commands:

Subgraph, all authors:

```bash
python scripts/generate_affiliation_treemap.py
```

Fullgraph, all authors:

```bash
python scripts/generate_affiliation_treemap.py \
  --authors-csv data/dblp_ai_authors_2015_2025/authors_orcid_fullgraph.csv
```

Fullgraph, first author only:

```bash
python scripts/generate_affiliation_treemap.py \
  --authors-csv data/dblp_ai_authors_2015_2025/authors_orcid_fullgraph.csv \
  --author-scope first
```

The standard output stems are:

- `affiliation_paper_count_treemap.*`
- `affiliation_paper_count_treemap_fullgraph.*`
- `affiliation_paper_count_treemap_fullgraph_first_author.*`

where each `*` is a `.csv` count table and a `.png` static treemap image.

### 5. Interactive Plotly Treemap Pipeline

For browser-based interactive treemaps, use:

```bash
python scripts/generate_affiliation_treemap_plotly.py
```

By default, this script reads the three standard treemap CSVs under `data/dblp_ai_authors_<start>_<end>/results/` and writes matching `.html` files in the same directory:

- `affiliation_paper_count_treemap.html`
- `affiliation_paper_count_treemap_fullgraph.html`
- `affiliation_paper_count_treemap_fullgraph_first_author.html`

The interactive view supports:

- click-to-zoom treemap navigation by country and affiliation
- larger centered labels with dynamic font sizing and line wrapping
- a right-side details panel showing the clicked affiliation's top authors

Useful options:

- `--results-dir`: point to a different dataset result directory
- `--height 920`: control page height
- `--top-authors 12`: control how many top authors appear in the details panel

### 6. Bridge Author Visualization Pipeline

The bridge-author visualization pipeline is implemented in:

```bash
scripts/visualize_bridge_authors.py
```

This pipeline focuses on identifying and visualizing authors who connect different collaboration communities. These authors are treated as potential bridge authors because they have substantial coauthorship links across community boundaries.

The pipeline uses the same exported DBLP coauthor data as the general coauthor visualization:

```text
authors.csv
edges.csv
```

The current default input directory is:

```text
data/dblp_ai_authors_2015_2025
```

The pipeline works as follows:

1. Read `authors.csv` and `edges.csv`.
2. Build a NetworkX undirected weighted coauthor graph.
3. Add one node for each author.
4. Add one edge for each coauthor pair.
5. Store the coauthorship count as the edge `weight`.
6. Keep only the largest connected component of the graph.
7. Detect collaboration communities using Louvain community detection:

```text
nx.community.louvain_communities(component, weight="weight", seed=seed)
```

8. Assign each author in the largest component to a community.
9. For each author, compute bridge-related metrics:

| Metric | Description |
| --- | --- |
| `weighted_degree` | Total weighted collaboration strength of the author |
| `internal_weight` | Total collaboration weight with authors in the same community |
| `external_weight` | Total collaboration weight with authors in other communities |
| `external_ratio` | Share of collaboration weight going outside the author's own community |
| `community_span` | Number of distinct external communities connected by the author |
| `bridge_score` | Composite score used to rank bridge authors |

The bridge score is computed as:

```text
bridge_score = external_weight * (1 + log1p(community_span)) * external_ratio
```

This score rewards authors who:

```text
1. Have strong cross-community collaborations
2. Send a large share of their collaboration weight outside their own community
3. Connect to multiple external communities
```

This is different from simply ranking authors by degree. A highly connected author may have many collaborations inside a single community, while a bridge author connects otherwise separated communities.

10. Rank authors by:

```text
bridge_score descending
external_weight descending
paper_count descending
name ascending
```

11. Select the top bridge authors. By default:

```text
--top-bridge-k 25
```

12. For each selected bridge author, keep the strongest cross-community neighbors. By default:

```text
--cross-neighbors-per-bridge 3
```

13. For each selected bridge author, also keep a small number of same-community neighbors for context. By default:

```text
--same-neighbors-per-bridge 1
```

14. Build a plot subgraph from the selected bridge authors and their selected neighbors.
15. Remove plotted edges with weight lower than `--min-edge-weight`.
16. Remove isolated context nodes, while keeping selected bridge authors.
17. Compute a spring layout for the plot graph.
18. Draw the bridge-author visualization:

```text
node = author
node color = detected community
dark node outline = selected bridge author
orange edge = cross-community collaboration
gray edge = same-community collaboration
bridge node size = bridge_score
edge width = coauthorship weight
label = selected bridge author name
```

19. Add a small text summary of the top bridge authors to the plot.
20. Save the bridge-author PNG and CSV ranking.

The default output files are:

| File | Description |
| --- | --- |
| `bridge_authors.png` | Bridge-author network visualization |
| `bridge_authors.csv` | Ranking table of the top bridge authors |

Example command for the 2015-2025 dataset:

```bash
python scripts/visualize_bridge_authors.py --input-dir data/dblp_ai_authors_2015_2025
```

Using the `modal` conda environment:

```bash
conda run -n modal python scripts/visualize_bridge_authors.py --input-dir data/dblp_ai_authors_2015_2025
```

Example with custom bridge visualization parameters:

```bash
python scripts/visualize_bridge_authors.py \
  --input-dir data/dblp_ai_authors_2015_2025 \
  --top-bridge-k 40 \
  --cross-neighbors-per-bridge 4 \
  --same-neighbors-per-bridge 2 \
  --min-edge-weight 2
```

The bridge-author visualization is designed to answer:

```text
Who connects otherwise separate collaboration communities?
```

It complements `visualize_coauthor_graph.py`, which provides a high-level overview of the most connected part of the collaboration network.

## Current Working Pipeline (May 2026)

The current analysis workflow used in this repository is:

### Step 1. Build the full DBLP coauthor graph

Run:

```bash
python scripts/fetch_dblp_ai_coauthor_graph.py --start-year 2015 --end-year 2025
```

What this step does:

1. Fetch DBLP proceedings and per-year TOC XML files.
2. Extract paper metadata and coauthor lists.
3. Build a full undirected weighted coauthor graph.
4. Extract ORCID from paper-level DBLP author entries when available.
5. If a paper-level ORCID is missing, try to recover it from the DBLP person record using the DBLP `pid`.
6. Save checkpoint files after each venue-year pair.

Main outputs:

- `authors.csv`
- `edges.csv`
- `papers.csv`
- `graph.graphml`
- `summary.json`

Notes:

- `authors.csv` stores the final author-level `orcid` when available.
- `papers.csv` stores both raw paper-level ORCID values and the resolved ORCID values after fallback.

### Step 2. Backfill ORCID from the local DBLP dump

We use the downloaded full DBLP dump under:

```text
dblp_data/dblp.xml.gz
```

Run:

```bash
python scripts/backfill_orcid_from_pid.py \
  --input-csv data/dblp_ai_authors_2015_2025/authors_filtered_full_graph.csv \
  --dblp-xml-gz dblp_data/dblp.xml.gz
```

What this step does:

1. Scan local `<www key="homepages/<pid>"> ... </www>` blocks in the DBLP dump.
2. Extract ORCID from homepage records when a URL of the form `https://orcid.org/...` exists.
3. Backfill author ORCID values locally without relying on online API calls.

Main output:

- `authors_filtered_full_graph_orcid_backfilled.csv`

### Step 3. Build the ORCID + affiliation subgraph

Run:

```bash
python scripts/generate_orcid_subgraph_communities.py \
  --input-dir data/dblp_ai_authors_2015_2025 \
  --dblp-xml-gz dblp_data/dblp.xml.gz \
  --min-edge-weight 3
```

What this step does:

1. Start from the full author list.
2. Load or build a local `pid -> ORCID` cache from `dblp.xml.gz`.
3. Keep authors with resolved ORCID.
4. Query OpenAlex by ORCID to obtain affiliation information.
5. Keep only authors whose `affiliation` is non-empty.
6. Build the induced coauthor subgraph on these authors.
7. Remove all edges with `weight < 3`.
8. Remove isolated nodes after edge filtering.
9. Detect communities on the resulting subgraph using **Louvain**:

```python
nx.community.louvain_communities(graph, weight="weight", seed=42)
```

Main outputs:

- `authors_orcid_backfilled.csv`
- `authors_orcid_subgraph.csv`
- `edges_orcid_subgraph.csv`
- `community_assignments_orcid_subgraph.csv`
- `local_pid_orcid_cache.csv`

Important details:

- `authors_orcid_subgraph.csv` contains:
  - `affiliation`: one primary affiliation used as the single-label field
  - `all_affiliations`: the full OpenAlex affiliation list separated by `|`
- The current working subgraph therefore contains:
  - authors with ORCID
  - authors with non-empty affiliation
  - only collaboration edges with weight at least `3`
  - no isolates

### Step 3b. Build an affiliation -> country lookup table

After affiliation extraction, we build a separate institution-country lookup table.

Run:

```bash
python scripts/build_affiliation_country_dict.py \
  --authors-csv data/dblp_ai_authors_2015_2025/authors_orcid_subgraph.csv
```

What this step does:

1. Read all ORCID authors from `authors_orcid_subgraph.csv`.
2. Collect both:
   - the primary `affiliation`
   - every label appearing in `all_affiliations`
3. Deduplicate these affiliation strings into a unique institution list.
4. Query **ROR** for each unique affiliation string.
5. Extract:
   - organization ID
   - organization name
   - `country_code`
   - `country_name`
6. If ROR does not resolve a label reliably, use a small manual override table for known edge cases.
7. Save the result as both a CSV table and a JSON dictionary.

Main outputs:

- `unique_affiliations.csv`
- `affiliation_country_lookup.csv`
- `affiliation_country_dict.json`

Important details:

- `affiliation_country_lookup.csv` is the flat table version.
- `affiliation_country_dict.json` is the dict-style version that can be loaded directly in Python.
- `match_status` may be:
  - `matched`: resolved automatically by ROR
  - `manual`: resolved through a manual override
  - `not_found`: still unresolved
  - `error:...`: lookup failed

Example JSON structure:

```json
{
  "Shanghai Jiao Tong University": {
    "ror_id": "https://ror.org/...",
    "ror_name": "Shanghai Jiao Tong University",
    "country_code": "CN",
    "country_name": "China",
    "match_status": "matched"
  }
}
```

### Step 4. Compute community purity using `all_affiliations`

Run:

```bash
python scripts/compute_affiliation_purity.py \
  --community-csv data/dblp_ai_authors_2015_2025/community_assignments_orcid_subgraph.csv \
  --authors-csv data/dblp_ai_authors_2015_2025/authors_orcid_subgraph.csv
```

What this step does:

1. Use detected `community_id` as the predicted clustering.
2. Use `all_affiliations` as the ground-truth label set.
3. Split each `all_affiliations` field by `|`.
4. For each community, count how many authors contain each affiliation label.
5. Take the dominant affiliation count in each community.
6. Compute purity as:

```text
Purity = (sum of dominant affiliation counts over all communities) / (number of evaluated authors)
```

Main outputs:

- `community_affiliation_purity.csv`
- `community_affiliation_purity_summary.json`

Interpretation:

- `affiliation` purity measures how strongly communities align with institutional structure.
- `all_affiliations` purity is more permissive than single-label purity, because one author may contribute to multiple institution labels.

### Summary of the current default analysis object

The community analysis currently reported in this repository is performed on:

- authors with ORCID
- authors with non-empty OpenAlex affiliation
- the induced coauthor subgraph after filtering to `weight >= 3`
- isolates removed
- Louvain communities on the resulting weighted graph
- purity evaluated against `all_affiliations`

## Data Directory Guide

The main working directory is:

```text
data/dblp_ai_authors_2015_2025
```

The most important files currently used in the pipeline are:

| File | Description |
| --- | --- |
| `authors.csv` | Full author table exported directly from DBLP graph construction |
| `edges.csv` | Full weighted coauthor edge table exported directly from DBLP graph construction |
| `papers.csv` | Full paper metadata table |
| `graph.graphml` | Full graph in GraphML format |
| `summary.json` | Summary statistics for the full DBLP-derived graph |
| `authors_orcid_backfilled.csv` | Full author table after local DBLP ORCID backfill |
| `authors_orcid_subgraph.csv` | Current analysis author table after ORCID filtering, affiliation filtering, edge filtering, and isolate removal |
| `edges_orcid_subgraph.csv` | Current analysis edge table for the ORCID + affiliation subgraph |
| `community_assignments_orcid_subgraph.csv` | Louvain community assignments for the current ORCID + affiliation subgraph |
| `community_affiliation_purity.csv` | Per-community purity report using `all_affiliations` |
| `community_affiliation_purity_summary.json` | Overall purity summary |
| `local_pid_orcid_cache.csv` | Local cache of `dblp_pid -> ORCID` extracted from `dblp.xml.gz` |
| `unique_affiliations.csv` | Deduplicated list of affiliation labels extracted from ORCID authors |
| `affiliation_country_lookup.csv` | Affiliation-to-country lookup table built from ROR and manual overrides |
| `affiliation_country_dict.json` | JSON dictionary version of the affiliation-country mapping |

If you only need the current final analysis object, the most relevant files are:

- `authors_orcid_subgraph.csv`
- `edges_orcid_subgraph.csv`
- `community_assignments_orcid_subgraph.csv`
- `community_affiliation_purity_summary.json`
- `affiliation_country_lookup.csv`

## Basic statics
### Graph Scale
- Number of authors: 67,224
- Number of papers: 44,123
- Number of edges: 407,230
- Network density: 1.8023e-4 $2E / (N(N-1))$

### Collaboration Structure
- Degree: 
  - min: 1
  - median: 7
  - mean: 12.14
  - max: 535
- Weighted degree
  - min: 1
  - median: 7
  - mean: 16.10
  - max: 1100
- Edge weight distribution
  - min: 1
  - median: 1
  - mean: 16.10
  - max: 1100
  - weight=1: 338,151， 83.04%
  - weight=2: 42,809， 10.51%
  - weight=3: 12,960， 3.18%
  - weight=4: 5,802， 1.42%
  - weight=5: 2,815， 0.69%
- Average clustering coefficient $C_{i} = \frac{2T_i}{k_i(k_i-1)}$ : 0.7476

### Network Position
- PageRank:
  $PR(i) = \frac{1-d}{N} + d \sum_{j \in \mathcal{N}(i)} \frac{PR(j)}{k_j}$
[PageRank](data/dblp_ai_authors_2015_2025/results/coauthor_pagerank_summary.json)
- Betweenness centrality:
[Betweenness](data/dblp_ai_authors_2015_2025/results/coauthor_betweenness_summary.json)
- Eigenvector centrality:
[Eigenvector](data/dblp_ai_authors_2015_2025/results/coauthor_eigenvector_summary.json)

### Community and Evolution
- Community size
- Modularity
- Internal / external edge ratio
- Cross-community paper ratio
- New authors per year
- New collaboration edges per year
- Average team size per year
