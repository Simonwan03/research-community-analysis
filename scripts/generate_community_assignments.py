#!/usr/bin/env python3
"""Generate full-graph community assignments from exported coauthor data."""

from __future__ import annotations

import argparse
from pathlib import Path

import networkx as nx
import pandas as pd

from visualize_coauthor_graph import (
    build_graph,
    compute_bridge_scores,
    detect_communities,
    save_community_assignments,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate community assignments for the full filtered coauthor graph."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/dblp_ai_authors_2025_2025"),
        help="Directory containing authors.csv and edges.csv.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path. Defaults to <input-dir>/community_assignments_full.csv.",
    )
    parser.add_argument(
        "--filtered-authors-output",
        type=Path,
        default=None,
        help="Output CSV path for authors remaining after full-graph filtering. Defaults to <input-dir>/authors_filtered_full_graph.csv.",
    )
    parser.add_argument(
        "--min-edge-weight",
        type=int,
        default=3,
        help="Filter out edges lighter than this weight before community detection.",
    )
    return parser.parse_args()


def filter_graph_by_edge_weight(graph, min_edge_weight: int):
    filtered_graph = graph.copy()
    filtered_graph.remove_edges_from(
        [
            (u, v)
            for u, v, data in filtered_graph.edges(data=True)
            if data.get("weight", 1) < min_edge_weight
        ]
    )
    filtered_graph.remove_nodes_from(list(nx.isolates(filtered_graph)))
    return filtered_graph


def save_filtered_authors(filtered_graph, output_path: Path) -> None:
    rows = []
    weighted_degree = dict(filtered_graph.degree(weight="weight"))
    for node in sorted(
        filtered_graph.nodes(),
        key=lambda item: (
            weighted_degree.get(item, 0.0),
            filtered_graph.nodes[item].get("paper_count", 0),
            filtered_graph.nodes[item].get("name", item),
        ),
        reverse=True,
    ):
        rows.append(
            {
                "author_id": node,
                "name": filtered_graph.nodes[node].get("name", node),
                "dblp_pid": filtered_graph.nodes[node].get("dblp_pid", ""),
                "orcid": filtered_graph.nodes[node].get("orcid", ""),
                "paper_count": int(filtered_graph.nodes[node].get("paper_count", 0)),
                "weighted_degree": int(weighted_degree.get(node, 0.0)),
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_path, index=False, encoding="utf-8")


def main() -> None:
    args = parse_args()
    output_path = args.output or (args.input_dir / "community_assignments_full.csv")
    filtered_authors_output = args.filtered_authors_output or (
        args.input_dir / "authors_filtered_full_graph.csv"
    )

    graph, _ = build_graph(args.input_dir)
    filtered_graph = filter_graph_by_edge_weight(graph, args.min_edge_weight)
    if filtered_graph.number_of_nodes() == 0:
        raise SystemExit("Graph is empty after filtering. Try a lower --min-edge-weight.")

    save_filtered_authors(filtered_graph, filtered_authors_output)
    weighted_degree = dict(filtered_graph.degree(weight="weight"))
    community_membership = detect_communities(filtered_graph)
    bridge_scores = compute_bridge_scores(filtered_graph, community_membership)

    # Pass the full filtered graph twice so every retained node is marked as visualized.
    save_community_assignments(
        output_path,
        filtered_graph,
        filtered_graph,
        community_membership,
        weighted_degree,
        bridge_scores,
    )

    print(
        f"Saved filtered authors to {filtered_authors_output}. "
        f"Saved full-graph community assignments to {output_path} "
        f"for {filtered_graph.number_of_nodes()} authors and {filtered_graph.number_of_edges()} edges."
    )


if __name__ == "__main__":
    main()
