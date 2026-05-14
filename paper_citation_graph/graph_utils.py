"""Helpers for exporting an internal citation subgraph."""

from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

import networkx as nx

from matcher import LocalPaper


def build_internal_citation_graph(
    local_papers: dict[str, LocalPaper],
    resolved_rows: list[dict[str, Any]],
    internal_edges: list[dict[str, Any]],
) -> nx.DiGraph:
    """Build a directed graph containing only papers from the local CSV."""

    graph = nx.DiGraph()
    resolved_by_local_id = {row["local_input_id"]: row for row in resolved_rows}

    for local_id, local_paper in local_papers.items():
        resolved = resolved_by_local_id.get(local_id, {})
        graph.add_node(
            local_id,
            local_id=local_id,
            paper_id=local_id,
            title=local_paper.title,
            year=local_paper.year if local_paper.year is not None else "",
            venue=local_paper.venue or "",
            authors="|".join(local_paper.authors or []),
            dblp_key=local_paper.dblp_key or "",
            ee=local_paper.ee or "",
            dblp_url=local_paper.dblp_url or "",
            doi=local_paper.doi or "",
            semantic_scholar_paper_id=resolved.get("matched_paper_id") or "",
            semantic_scholar_match_status=resolved.get("match_status") or "",
            semantic_scholar_match_score=resolved.get("match_score") if resolved else "",
        )

    for edge in internal_edges:
        graph.add_edge(
            edge["source_local_id"],
            edge["target_local_id"],
            source_semantic_scholar_paper_id=edge["source_paper_id"],
            target_semantic_scholar_paper_id=edge["target_paper_id"],
            edge_type="references",
            source="semantic_scholar",
        )

    return graph


def write_internal_edges_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "source_local_id",
        "target_local_id",
        "source_paper_id",
        "target_paper_id",
        "source_title",
        "target_title",
        "source_year",
        "target_year",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def write_summary(
    path: Path,
    graph: nx.DiGraph,
    seed_count: int,
    matched_count: int,
    unmatched_count: int,
    ambiguous_count: int,
) -> None:
    yearly_counts = Counter(
        int(data["year"])
        for _, data in graph.nodes(data=True)
        if str(data.get("year", "")).isdigit()
    )
    summary = {
        "number_of_local_papers": seed_count,
        "matched_papers": matched_count,
        "unmatched_papers": unmatched_count,
        "ambiguous_papers": ambiguous_count,
        "number_of_nodes": graph.number_of_nodes(),
        "number_of_edges": graph.number_of_edges(),
        "weakly_connected_components_count": nx.number_weakly_connected_components(graph) if graph.number_of_nodes() else 0,
        "density": nx.density(graph) if graph.number_of_nodes() > 1 else 0.0,
        "yearly_paper_count": dict(sorted(yearly_counts.items())),
        "top_internal_cited_papers": [
            {
                "local_id": node,
                "title": graph.nodes[node].get("title", ""),
                "year": graph.nodes[node].get("year", ""),
                "in_degree": degree,
            }
            for node, degree in sorted(graph.in_degree(), key=lambda item: (-item[1], str(item[0])))[:20]
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
