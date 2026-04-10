#!/usr/bin/env python3
"""Visualize a manageable NetworkX subgraph from exported coauthor data."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import tempfile

os.environ.setdefault(
    "MPLCONFIGDIR",
    str(Path(tempfile.gettempdir()) / "research-community-analysis-mpl"),
)

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Visualize a coauthor graph exported by fetch_dblp_ai_coauthor_graph.py."
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
        help="Output PNG path. Defaults to <input-dir>/coauthor_top120.png.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=120,
        help="Keep the top-k authors by weighted degree for visualization.",
    )
    parser.add_argument(
        "--label-top-k",
        type=int,
        default=20,
        help="Label only the top-k authors inside the displayed subgraph.",
    )
    parser.add_argument(
        "--min-edge-weight",
        type=int,
        default=1,
        help="Filter out edges lighter than this weight before plotting.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for spring layout.",
    )
    return parser.parse_args()


def build_graph(input_dir: Path) -> tuple[nx.Graph, pd.DataFrame]:
    authors_path = input_dir / "authors.csv"
    edges_path = input_dir / "edges.csv"

    if not authors_path.exists():
        raise FileNotFoundError(f"Missing file: {authors_path}")
    if not edges_path.exists():
        raise FileNotFoundError(f"Missing file: {edges_path}")

    authors_df = pd.read_csv(authors_path)
    edges_df = pd.read_csv(edges_path)

    graph = nx.Graph()
    for row in authors_df.itertuples(index=False):
        graph.add_node(
            row.author_id,
            name=row.name,
            paper_count=int(row.paper_count),
            dblp_pid=row.dblp_pid if isinstance(row.dblp_pid, str) else "",
        )

    for row in edges_df.itertuples(index=False):
        graph.add_edge(
            row.source_author_id,
            row.target_author_id,
            weight=int(row.weight),
            paper_count=int(row.paper_count),
        )

    return graph, authors_df


def select_subgraph(
    graph: nx.Graph,
    authors_df: pd.DataFrame,
    top_k: int,
    min_edge_weight: int,
) -> tuple[nx.Graph, dict[str, float]]:
    weighted_degree = dict(graph.degree(weight="weight"))
    authors_df = authors_df.copy()
    authors_df["weighted_degree"] = authors_df["author_id"].map(weighted_degree).fillna(0)

    top_nodes = (
        authors_df.sort_values(
            by=["weighted_degree", "paper_count", "name"],
            ascending=[False, False, True],
        )
        .head(top_k)["author_id"]
        .tolist()
    )

    subgraph = graph.subgraph(top_nodes).copy()
    light_edges = [
        (u, v)
        for u, v, data in subgraph.edges(data=True)
        if data.get("weight", 1) < min_edge_weight
    ]
    subgraph.remove_edges_from(light_edges)
    isolates = list(nx.isolates(subgraph))
    subgraph.remove_nodes_from(isolates)

    if subgraph.number_of_nodes() == 0:
        raise ValueError("Subgraph is empty after filtering. Try a larger --top-k or lower --min-edge-weight.")

    largest_component_nodes = max(nx.connected_components(subgraph), key=len)
    subgraph = subgraph.subgraph(largest_component_nodes).copy()
    return subgraph, weighted_degree


def detect_communities(subgraph: nx.Graph) -> dict[str, int]:
    if subgraph.number_of_nodes() <= 1:
        return {node: 0 for node in subgraph.nodes()}

    communities = list(nx.community.greedy_modularity_communities(subgraph, weight="weight"))
    membership: dict[str, int] = {}
    for community_id, community_nodes in enumerate(communities):
        for node in community_nodes:
            membership[node] = community_id
    return membership


def scale(values: list[float], low: float, high: float) -> list[float]:
    if not values:
        return []
    min_value = min(values)
    max_value = max(values)
    if min_value == max_value:
        midpoint = (low + high) / 2
        return [midpoint for _ in values]
    return [
        low + (value - min_value) * (high - low) / (max_value - min_value)
        for value in values
    ]


def plot_subgraph(
    subgraph: nx.Graph,
    weighted_degree: dict[str, float],
    output_path: Path,
    label_top_k: int,
    seed: int,
) -> None:
    community_membership = detect_communities(subgraph)
    positions = nx.spring_layout(subgraph, k=0.45, iterations=200, weight="weight", seed=seed)

    node_values = [weighted_degree.get(node, 0.0) for node in subgraph.nodes()]
    node_sizes = scale(node_values, 180, 2200)
    node_colors = [community_membership[node] for node in subgraph.nodes()]
    edge_widths = scale(
        [data.get("weight", 1) for _, _, data in subgraph.edges(data=True)],
        0.5,
        4.0,
    )

    plt.figure(figsize=(16, 12))
    nx.draw_networkx_edges(
        subgraph,
        positions,
        width=edge_widths,
        alpha=0.25,
        edge_color="#6b7280",
    )
    nx.draw_networkx_nodes(
        subgraph,
        positions,
        node_size=node_sizes,
        node_color=node_colors,
        cmap=plt.cm.Set2,
        linewidths=0.6,
        edgecolors="white",
        alpha=0.92,
    )

    label_nodes = sorted(
        subgraph.nodes(),
        key=lambda node: weighted_degree.get(node, 0.0),
        reverse=True,
    )[:label_top_k]
    labels = {
        node: subgraph.nodes[node].get("name", node)
        for node in label_nodes
    }
    nx.draw_networkx_labels(
        subgraph,
        positions,
        labels=labels,
        font_size=8,
        font_weight="bold",
        font_family="DejaVu Sans",
    )

    plt.title(
        f"Coauthor Network Subgraph ({subgraph.number_of_nodes()} authors, "
        f"{subgraph.number_of_edges()} collaborations)",
        fontsize=16,
        pad=18,
    )
    plt.axis("off")
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=240, bbox_inches="tight")
    plt.close()


def main() -> None:
    args = parse_args()
    output_path = args.output or (args.input_dir / f"coauthor_top{args.top_k}.png")

    graph, authors_df = build_graph(args.input_dir)
    subgraph, weighted_degree = select_subgraph(
        graph,
        authors_df,
        top_k=args.top_k,
        min_edge_weight=args.min_edge_weight,
    )
    plot_subgraph(
        subgraph,
        weighted_degree,
        output_path=output_path,
        label_top_k=args.label_top_k,
        seed=args.seed,
    )

    print(
        f"Saved visualization to {output_path} "
        f"with {subgraph.number_of_nodes()} nodes and {subgraph.number_of_edges()} edges."
    )


if __name__ == "__main__":
    main()
