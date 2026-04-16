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
```
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