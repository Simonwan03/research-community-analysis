#!/usr/bin/env python3
"""Visualize community-colored bridge authors from exported coauthor data."""

from __future__ import annotations

import argparse
import math
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
        description="Visualize community-colored bridge authors from coauthor CSV exports."
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
        help="Output PNG path. Defaults to <input-dir>/bridge_authors.png.",
    )
    parser.add_argument(
        "--top-bridge-k",
        type=int,
        default=25,
        help="Number of bridge authors to highlight.",
    )
    parser.add_argument(
        "--cross-neighbors-per-bridge",
        type=int,
        default=3,
        help="Strongest cross-community neighbors to keep for each bridge author.",
    )
    parser.add_argument(
        "--same-neighbors-per-bridge",
        type=int,
        default=1,
        help="Strongest same-community neighbors to keep for context.",
    )
    parser.add_argument(
        "--min-edge-weight",
        type=int,
        default=1,
        help="Drop plotted edges lighter than this weight.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for layout and community detection.",
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
        )

    for row in edges_df.itertuples(index=False):
        graph.add_edge(
            row.source_author_id,
            row.target_author_id,
            weight=int(row.weight),
            paper_count=int(row.paper_count),
        )

    return graph, authors_df


def detect_communities(graph: nx.Graph, seed: int) -> tuple[nx.Graph, dict[str, int]]:
    component_nodes = max(nx.connected_components(graph), key=len)
    component = graph.subgraph(component_nodes).copy()
    communities = nx.community.louvain_communities(component, weight="weight", seed=seed)
    membership: dict[str, int] = {}
    for community_id, nodes in enumerate(communities):
        for node in nodes:
            membership[node] = community_id
    return component, membership


def compute_bridge_table(
    component: nx.Graph,
    membership: dict[str, int],
    authors_df: pd.DataFrame,
) -> pd.DataFrame:
    records: list[dict] = []
    author_name_map = authors_df.set_index("author_id")["name"].to_dict()
    paper_count_map = authors_df.set_index("author_id")["paper_count"].to_dict()

    for node in component.nodes():
        weighted_degree = 0
        internal_weight = 0
        external_weight = 0
        external_communities: set[int] = set()
        for neighbor, edge_data in component[node].items():
            weight = int(edge_data.get("weight", 1))
            weighted_degree += weight
            if membership[neighbor] == membership[node]:
                internal_weight += weight
            else:
                external_weight += weight
                external_communities.add(membership[neighbor])

        external_ratio = external_weight / weighted_degree if weighted_degree else 0.0
        community_span = len(external_communities)
        bridge_score = external_weight * (1.0 + math.log1p(community_span)) * external_ratio

        records.append(
            {
                "author_id": node,
                "name": author_name_map.get(node, node),
                "community_id": membership[node],
                "paper_count": int(paper_count_map.get(node, 0)),
                "weighted_degree": weighted_degree,
                "internal_weight": internal_weight,
                "external_weight": external_weight,
                "external_ratio": external_ratio,
                "community_span": community_span,
                "bridge_score": bridge_score,
            }
        )

    bridge_df = pd.DataFrame(records).sort_values(
        by=["bridge_score", "external_weight", "paper_count", "name"],
        ascending=[False, False, False, True],
    )
    return bridge_df


def choose_plot_nodes(
    component: nx.Graph,
    membership: dict[str, int],
    bridge_df: pd.DataFrame,
    top_bridge_k: int,
    cross_neighbors_per_bridge: int,
    same_neighbors_per_bridge: int,
) -> tuple[set[str], set[str]]:
    bridge_nodes = set(bridge_df.head(top_bridge_k)["author_id"].tolist())
    selected_nodes = set(bridge_nodes)

    for node in bridge_nodes:
        cross_neighbors: list[tuple[str, int]] = []
        same_neighbors: list[tuple[str, int]] = []
        for neighbor, edge_data in component[node].items():
            weight = int(edge_data.get("weight", 1))
            bucket = cross_neighbors if membership[neighbor] != membership[node] else same_neighbors
            bucket.append((neighbor, weight))

        cross_neighbors.sort(key=lambda item: (-item[1], component.nodes[item[0]].get("paper_count", 0)))
        same_neighbors.sort(key=lambda item: (-item[1], component.nodes[item[0]].get("paper_count", 0)))

        selected_nodes.update(neighbor for neighbor, _ in cross_neighbors[:cross_neighbors_per_bridge])
        selected_nodes.update(neighbor for neighbor, _ in same_neighbors[:same_neighbors_per_bridge])

    return selected_nodes, bridge_nodes


def scale(values: list[float], low: float, high: float) -> list[float]:
    if not values:
        return []
    min_value = min(values)
    max_value = max(values)
    if min_value == max_value:
        return [(low + high) / 2 for _ in values]
    return [low + (value - min_value) * (high - low) / (max_value - min_value) for value in values]


def plot_bridge_authors(
    component: nx.Graph,
    membership: dict[str, int],
    bridge_df: pd.DataFrame,
    selected_nodes: set[str],
    bridge_nodes: set[str],
    output_path: Path,
    min_edge_weight: int,
    seed: int,
) -> pd.DataFrame:
    plot_graph = component.subgraph(selected_nodes).copy()
    edges_to_drop = [
        (u, v)
        for u, v, data in plot_graph.edges(data=True)
        if int(data.get("weight", 1)) < min_edge_weight
    ]
    plot_graph.remove_edges_from(edges_to_drop)

    isolates = [node for node in nx.isolates(plot_graph) if node not in bridge_nodes]
    plot_graph.remove_nodes_from(isolates)

    if plot_graph.number_of_nodes() == 0:
        raise ValueError("Plot graph is empty after filtering.")

    bridge_score_map = bridge_df.set_index("author_id")["bridge_score"].to_dict()
    positions = nx.spring_layout(plot_graph, k=0.9, iterations=250, weight="weight", seed=seed)

    plt.figure(figsize=(16, 12))
    edge_colors = []
    edge_width_values = []
    for u, v, data in plot_graph.edges(data=True):
        cross_community = membership[u] != membership[v]
        edge_colors.append("#d97706" if cross_community else "#94a3b8")
        edge_width_values.append(int(data.get("weight", 1)) * (1.6 if cross_community else 0.8))
    edge_widths = scale(edge_width_values, 0.8, 4.8)

    nx.draw_networkx_edges(
        plot_graph,
        positions,
        width=edge_widths,
        edge_color=edge_colors,
        alpha=0.35,
    )

    context_nodes = [node for node in plot_graph.nodes() if node not in bridge_nodes]
    bridge_plot_nodes = [node for node in plot_graph.nodes() if node in bridge_nodes]

    if context_nodes:
        nx.draw_networkx_nodes(
            plot_graph,
            positions,
            nodelist=context_nodes,
            node_size=scale([bridge_score_map.get(node, 0.0) for node in context_nodes], 180, 700),
            node_color=[membership[node] for node in context_nodes],
            cmap=plt.cm.Set2,
            linewidths=0.5,
            edgecolors="white",
            alpha=0.88,
        )

    if bridge_plot_nodes:
        nx.draw_networkx_nodes(
            plot_graph,
            positions,
            nodelist=bridge_plot_nodes,
            node_size=scale([bridge_score_map.get(node, 0.0) for node in bridge_plot_nodes], 900, 2800),
            node_color=[membership[node] for node in bridge_plot_nodes],
            cmap=plt.cm.Set2,
            linewidths=1.8,
            edgecolors="#111827",
            alpha=0.96,
        )

    labels = {
        node: plot_graph.nodes[node].get("name", node)
        for node in bridge_plot_nodes
    }
    nx.draw_networkx_labels(
        plot_graph,
        positions,
        labels=labels,
        font_size=8,
        font_weight="bold",
        font_family="DejaVu Sans",
    )

    top_rows = bridge_df[bridge_df["author_id"].isin(bridge_nodes)].head(10)
    summary_lines = [
        f"{row.name} | score={row.bridge_score:.1f} | ext={int(row.external_weight)} | span={int(row.community_span)}"
        for row in top_rows.itertuples(index=False)
    ]
    plt.gcf().text(
        0.02,
        0.02,
        "Top bridge authors:\n" + "\n".join(summary_lines),
        fontsize=9,
        family="DejaVu Sans Mono",
        va="bottom",
    )

    plt.title(
        "Bridge Authors Across Communities\n"
        "Node color = community, dark outline = bridge author, orange edge = cross-community tie",
        fontsize=16,
        pad=18,
    )
    plt.axis("off")
    plt.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=240, bbox_inches="tight")
    plt.close()

    return bridge_df


def main() -> None:
    args = parse_args()
    output_path = args.output or (args.input_dir / "bridge_authors.png")

    graph, authors_df = build_graph(args.input_dir)
    component, membership = detect_communities(graph, seed=args.seed)
    bridge_df = compute_bridge_table(component, membership, authors_df)
    selected_nodes, bridge_nodes = choose_plot_nodes(
        component,
        membership,
        bridge_df,
        top_bridge_k=args.top_bridge_k,
        cross_neighbors_per_bridge=args.cross_neighbors_per_bridge,
        same_neighbors_per_bridge=args.same_neighbors_per_bridge,
    )
    bridge_df = plot_bridge_authors(
        component,
        membership,
        bridge_df,
        selected_nodes,
        bridge_nodes,
        output_path=output_path,
        min_edge_weight=args.min_edge_weight,
        seed=args.seed,
    )

    csv_path = output_path.with_suffix(".csv")
    bridge_df.head(args.top_bridge_k).to_csv(csv_path, index=False)
    print(
        f"Saved bridge-author visualization to {output_path} and rankings to {csv_path}. "
        f"Detected {bridge_df['community_id'].nunique()} communities in the largest component."
    )


if __name__ == "__main__":
    main()
