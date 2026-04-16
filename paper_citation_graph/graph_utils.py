"""Graph construction, export, and summary helpers for paper citation graphs."""

from __future__ import annotations

import json
import pickle
from collections import Counter
from pathlib import Path
from typing import Any

import networkx as nx


def paper_node_attributes(
    paper: dict[str, Any],
    is_seed_paper: bool,
    source_origin: str,
) -> dict[str, Any]:
    """Convert Semantic Scholar paper metadata into graph node attributes."""

    authors = paper.get("authors") or []
    author_names = [author.get("name", "") for author in authors if isinstance(author, dict)]
    return {
        "paper_id": paper.get("paperId") or "",
        "title": paper.get("title") or "",
        "year": paper.get("year") or "",
        "venue": paper.get("venue") or "",
        "authors": "|".join(author_names),
        "citation_count": paper.get("citationCount") if paper.get("citationCount") is not None else "",
        "reference_count": paper.get("referenceCount") if paper.get("referenceCount") is not None else "",
        "is_seed_paper": bool(is_seed_paper),
        "source_origin": source_origin,
        "url": paper.get("url") or "",
        "external_ids": json.dumps(paper.get("externalIds") or {}, ensure_ascii=False, sort_keys=True),
    }


def build_citation_graph(
    seed_metadata: dict[str, dict[str, Any]],
    reference_edges: list[dict[str, Any]],
) -> nx.DiGraph:
    """Build a directed citation graph from seed metadata and reference rows."""

    graph = nx.DiGraph()

    for paper_id, metadata in seed_metadata.items():
        graph.add_node(
            paper_id,
            **paper_node_attributes(metadata, is_seed_paper=True, source_origin="seed"),
        )

    for edge in reference_edges:
        source_id = edge.get("source_paper_id")
        target_id = edge.get("target_paper_id")
        if not source_id or not target_id:
            continue

        if source_id not in graph:
            graph.add_node(
                source_id,
                paper_id=source_id,
                title=edge.get("source_title") or "",
                year=edge.get("source_year") or "",
                venue="",
                authors="",
                citation_count="",
                reference_count="",
                is_seed_paper=True,
                source_origin="seed",
                url="",
                external_ids="{}",
            )

        if target_id not in graph:
            target_metadata = edge.get("target_metadata") or {
                "paperId": target_id,
                "title": edge.get("target_title"),
                "year": edge.get("target_year"),
                "authors": edge.get("target_authors") or [],
                "externalIds": edge.get("target_external_ids") or {},
                "venue": edge.get("target_venue"),
                "url": edge.get("target_url"),
                "citationCount": edge.get("target_citation_count"),
                "referenceCount": edge.get("target_reference_count"),
            }
            graph.add_node(
                target_id,
                **paper_node_attributes(target_metadata, is_seed_paper=False, source_origin="reference_only"),
            )

        graph.add_edge(
            source_id,
            target_id,
            edge_type="references",
            source="semantic_scholar",
        )

    return graph


def export_graph(graph: nx.DiGraph, graphml_path: Path, gpickle_path: Path | None = None) -> None:
    """Export graph to GraphML and optionally pickle."""

    graphml_path.parent.mkdir(parents=True, exist_ok=True)
    nx.write_graphml(graph, graphml_path)
    if gpickle_path is not None:
        with gpickle_path.open("wb") as handle:
            pickle.dump(graph, handle, protocol=pickle.HIGHEST_PROTOCOL)


def graph_summary(
    graph: nx.DiGraph,
    seed_paper_ids: set[str],
    seed_count: int,
    matched_count: int,
    unmatched_count: int,
    ambiguous_count: int,
) -> dict[str, Any]:
    """Compute summary statistics for the citation graph."""

    seed_out_degrees = [graph.out_degree(node) for node in seed_paper_ids if node in graph]
    average_out_degree = sum(seed_out_degrees) / len(seed_out_degrees) if seed_out_degrees else 0.0
    top_cited = sorted(graph.in_degree(), key=lambda item: (-item[1], graph.nodes[item[0]].get("title", "")))[:20]
    yearly_counts = Counter(
        int(data["year"])
        for _, data in graph.nodes(data=True)
        if str(data.get("year", "")).isdigit()
    )

    return {
        "number_of_seed_papers": seed_count,
        "matched_papers": matched_count,
        "unmatched_papers": unmatched_count,
        "ambiguous_papers": ambiguous_count,
        "number_of_nodes": graph.number_of_nodes(),
        "number_of_edges": graph.number_of_edges(),
        "average_out_degree_among_seed_papers": average_out_degree,
        "weakly_connected_components_count": nx.number_weakly_connected_components(graph),
        "in_degree_distribution": _degree_distribution(dict(graph.in_degree())),
        "out_degree_distribution": _degree_distribution(dict(graph.out_degree())),
        "yearly_paper_count": dict(sorted(yearly_counts.items())),
        "top_cited_nodes_inside_graph": [
            {
                "paper_id": paper_id,
                "in_degree": degree,
                "title": graph.nodes[paper_id].get("title", ""),
                "year": graph.nodes[paper_id].get("year", ""),
                "is_seed_paper": graph.nodes[paper_id].get("is_seed_paper", False),
            }
            for paper_id, degree in top_cited
        ],
    }


def _degree_distribution(degrees: dict[str, int]) -> dict[str, int]:
    counts = Counter(degrees.values())
    return {str(degree): count for degree, count in sorted(counts.items())}

