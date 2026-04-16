# Bridging Research Communities: A Network Analysis of Scientific Collaboration
This project explores community structure and bridge authors in scientific collaboration networks using graph analysis methods. More specificly, We construct a co-authorship network to study collaboration structure, and use paper abstracts and citation information as auxiliary signals to interpret community topics and measure cross-community influence.

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

If you want a coauthor graph under the definition:

`AI author = an author who published at least one paper in AAAI, IJCAI, ICML, NeurIPS, or ICLR during a chosen year range`

you can run:

```bash
python scripts/fetch_dblp_ai_coauthor_graph.py
```

By default, the script fetches `AAAI`, `IJCAI`, `ICML`, `NeurIPS`, and `ICLR`
from `2015` to the current year. It writes:

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

### 2. Coauthor Graph Visualization Pipeline

The coauthor graph visualization pipeline is implemented in:

```bash
scripts/visualize_coauthor_graph.py
```

It visualizes a manageable subgraph from the exported DBLP coauthor data. It does not draw the full graph by default, because the full graph can contain tens of thousands of authors and hundreds of thousands of edges.

The visualization pipeline uses:

```text
authors.csv
edges.csv
```

from an exported DBLP data directory.

The default input directory is:

```text
data/dblp_ai_authors_2025_2025
```

The pipeline works as follows:

1. Read `authors.csv` and `edges.csv`.
2. Build a NetworkX undirected weighted graph.
3. Add one node for each author.
4. Add one edge for each coauthor pair.
5. Store the coauthorship count as the edge `weight`.
6. Compute weighted degree for each author:

```text
weighted degree = total weighted collaboration strength of an author
```

7. Select the top-k authors by:

```text
weighted_degree descending
paper_count descending
name ascending
```

8. Build a subgraph induced by those top-k authors.
9. Remove edges with weight lower than `--min-edge-weight`.
10. Remove isolated nodes after edge filtering.
11. Keep only the largest connected component.
12. Detect communities using greedy modularity community detection.
13. Compute a spring layout for the selected subgraph.
14. Draw the graph:

```text
node = author
node size = weighted degree
node color = detected community
edge width = coauthorship weight
label = top authors by weighted degree
```

15. Save the final PNG image.

By default, the visualization keeps the top 120 authors:

```text
--top-k 120
```

and labels the top 20 authors:

```text
--label-top-k 20
```

The default output path is:

```text
<input-dir>/coauthor_top120.png
```

Example command for the 2015-2025 dataset:

```bash
python scripts/visualize_coauthor_graph.py --input-dir data/dblp_ai_authors_2015_2025
```

Using the `modal` conda environment:

```bash
conda run -n modal python scripts/visualize_coauthor_graph.py --input-dir data/dblp_ai_authors_2015_2025
```

Example with custom visualization parameters:

```bash
python scripts/visualize_coauthor_graph.py \
  --input-dir data/dblp_ai_authors_2015_2025 \
  --top-k 200 \
  --label-top-k 40 \
  --min-edge-weight 2
```

This visualization is intended as a high-level overview of the most connected authors and their collaboration communities. It is not a full-network visualization and it is not the bridge-author visualization. Bridge-author analysis is handled separately by:

```bash
scripts/visualize_bridge_authors.py
```

### 3. Bridge Author Visualization Pipeline

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

The default input directory is:

```text
data/dblp_ai_authors_2025_2025
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
