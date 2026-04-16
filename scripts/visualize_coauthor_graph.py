#!/usr/bin/env python3
"""Visualize a manageable NetworkX subgraph from exported coauthor data."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import tempfile

os.environ.setdefault(
    "MPLCONFIGDIR",
    str(Path(tempfile.gettempdir()) / "research-community-analysis-mpl"),
)

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
from matplotlib import patheffects
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
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
        default=10,
        help="Always label the global top-k authors, plus each community's top author.",
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
            orcid=row.orcid if hasattr(row, "orcid") and isinstance(row.orcid, str) else "",
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
    filtered_graph = graph.copy()
    filtered_graph.remove_edges_from(
        [
            (u, v)
            for u, v, data in filtered_graph.edges(data=True)
            if data.get("weight", 1) < min_edge_weight
        ]
    )
    filtered_graph.remove_nodes_from(list(nx.isolates(filtered_graph)))

    if filtered_graph.number_of_nodes() == 0:
        raise ValueError("Graph is empty after filtering. Try a lower --min-edge-weight.")

    largest_component_nodes = max(nx.connected_components(filtered_graph), key=len)
    component = filtered_graph.subgraph(largest_component_nodes).copy()

    weighted_degree = dict(component.degree(weight="weight"))
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

    subgraph = component.subgraph(top_nodes).copy()
    subgraph.remove_nodes_from(list(nx.isolates(subgraph)))

    if subgraph.number_of_nodes() == 0:
        raise ValueError(
            "Subgraph is empty after selecting top-k authors. "
            "Try a larger --top-k or lower --min-edge-weight."
        )

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


def clean_author_name(name: str) -> str:
    return re.sub(r"\s+\d{4}$", "", name).strip()


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


def build_community_color_map(community_ids: list[int]) -> dict[int, str]:
    base_palette = [
        "#3B82F6",
        "#F97316",
        "#10B981",
        "#EAB308",
        "#8B5CF6",
        "#EF4444",
        "#14B8A6",
        "#F59E0B",
        "#EC4899",
        "#6366F1",
        "#84CC16",
        "#06B6D4",
        "#A855F7",
        "#22C55E",
        "#FB7185",
        "#0EA5E9",
        "#F97316",
        "#65A30D",
        "#D946EF",
        "#64748B",
    ]
    unique_ids = sorted(set(community_ids))
    if len(unique_ids) > len(base_palette):
        extra_colors = list(mcolors.TABLEAU_COLORS.values())
        base_palette.extend(extra_colors)
    if len(unique_ids) > len(base_palette):
        raise ValueError("Not enough distinct colors available for the detected communities.")
    return {
        community_id: base_palette[index]
        for index, community_id in enumerate(unique_ids)
    }


def choose_label_nodes(
    subgraph: nx.Graph,
    community_membership: dict[str, int],
    weighted_degree: dict[str, float],
    label_top_k: int,
) -> list[str]:
    ranked_nodes = sorted(
        subgraph.nodes(),
        key=lambda node: (
            weighted_degree.get(node, 0.0),
            subgraph.nodes[node].get("paper_count", 0),
            clean_author_name(subgraph.nodes[node].get("name", node)),
        ),
        reverse=True,
    )
    selected = set(ranked_nodes[:label_top_k])

    community_nodes: dict[int, list[str]] = {}
    for node, community_id in community_membership.items():
        community_nodes.setdefault(community_id, []).append(node)

    for community_id in sorted(community_nodes):
        nodes = sorted(
            community_nodes[community_id],
            key=lambda node: (
                weighted_degree.get(node, 0.0),
                subgraph.nodes[node].get("paper_count", 0),
                clean_author_name(subgraph.nodes[node].get("name", node)),
            ),
            reverse=True,
        )
        selected.add(nodes[0])

    return sorted(
        selected,
        key=lambda node: (
            weighted_degree.get(node, 0.0),
            subgraph.nodes[node].get("paper_count", 0),
            clean_author_name(subgraph.nodes[node].get("name", node)),
        ),
        reverse=True,
    )


def compute_bridge_scores(
    subgraph: nx.Graph,
    community_membership: dict[str, int],
) -> dict[str, float]:
    bridge_scores: dict[str, float] = {}
    for node in subgraph.nodes():
        total_weight = 0
        external_weight = 0
        external_communities: set[int] = set()
        for neighbor, edge_data in subgraph[node].items():
            weight = int(edge_data.get("weight", 1))
            total_weight += weight
            if community_membership[neighbor] != community_membership[node]:
                external_weight += weight
                external_communities.add(community_membership[neighbor])

        if total_weight == 0:
            bridge_scores[node] = 0.0
            continue

        external_ratio = external_weight / total_weight
        bridge_scores[node] = external_weight * (1.0 + len(external_communities)) * external_ratio

    return bridge_scores


def normalize_positions(
    positions: dict[str, tuple[float, float]],
    margin: float = 0.05,
) -> dict[str, tuple[float, float]]:
    x_values = [x_coord for x_coord, _ in positions.values()]
    y_values = [y_coord for _, y_coord in positions.values()]
    min_x = min(x_values)
    max_x = max(x_values)
    min_y = min(y_values)
    max_y = max(y_values)
    width = max(max_x - min_x, 1e-6)
    height = max(max_y - min_y, 1e-6)
    usable = 1.0 - 2 * margin

    normalized = {}
    for node, (x_coord, y_coord) in positions.items():
        normalized[node] = (
            margin + ((x_coord - min_x) / width) * usable,
            margin + ((y_coord - min_y) / height) * usable,
        )
    return normalized


def manual_community_centers(num_communities: int) -> list[tuple[float, float]]:
    centers = [
        (0.16, 0.56),
        (0.34, 0.56),
        (0.54, 0.58),
        (0.73, 0.56),
        (0.88, 0.44),
        (0.72, 0.30),
        (0.52, 0.23),
        (0.32, 0.26),
        (0.18, 0.72),
        (0.48, 0.76),
        (0.74, 0.76),
        (0.90, 0.70),
    ]
    if num_communities <= len(centers):
        return centers[:num_communities]

    extra = []
    step = max(1, num_communities - len(centers))
    for index in range(step):
        extra.append((0.12 + 0.76 * (index / max(step - 1, 1)), 0.12))
    return centers + extra


def community_grouped_layout(
    subgraph: nx.Graph,
    community_membership: dict[str, int],
    seed: int,
) -> dict[str, tuple[float, float]]:
    communities: dict[int, list[str]] = {}
    for node, community_id in community_membership.items():
        communities.setdefault(community_id, []).append(node)

    ordered_communities = sorted(
        communities,
        key=lambda community_id: len(communities[community_id]),
        reverse=True,
    )
    center_lookup = {
        community_id: center
        for community_id, center in zip(
            ordered_communities,
            manual_community_centers(len(ordered_communities)),
        )
    }

    positions: dict[str, tuple[float, float]] = {}
    for community_id, nodes in communities.items():
        community_subgraph = subgraph.subgraph(nodes).copy()
        local_positions = nx.spring_layout(
            community_subgraph,
            k=2.3 / max(max(community_subgraph.number_of_nodes(), 1) ** 0.5, 1),
            iterations=350,
            weight="weight",
            scale=1.4,
            seed=seed,
        )

        scale_factor = 0.055 + 0.013 * (len(nodes) ** 0.5)
        center_x, center_y = center_lookup[community_id]
        for node, (x_coord, y_coord) in local_positions.items():
            positions[node] = (
                center_x + x_coord * scale_factor,
                center_y + y_coord * scale_factor,
            )

    return positions


def select_display_edges(
    subgraph: nx.Graph,
    community_membership: dict[str, int],
    intra_weight_threshold: int,
    inter_edges_per_pair: int,
) -> tuple[list[tuple[str, str, dict]], list[tuple[str, str, dict]]]:
    intra_edges: list[tuple[str, str, dict]] = []
    inter_edges_by_pair: dict[tuple[int, int], list[tuple[str, str, dict]]] = {}

    for left, right, data in subgraph.edges(data=True):
        left_community = community_membership[left]
        right_community = community_membership[right]
        weight = int(data.get("weight", 1))
        if left_community == right_community:
            if weight >= intra_weight_threshold:
                intra_edges.append((left, right, data))
            continue

        pair = tuple(sorted((left_community, right_community)))
        inter_edges_by_pair.setdefault(pair, []).append((left, right, data))

    inter_edges: list[tuple[str, str, dict]] = []
    for pair_edges in inter_edges_by_pair.values():
        pair_edges.sort(key=lambda item: int(item[2].get("weight", 1)), reverse=True)
        inter_edges.extend(pair_edges[:inter_edges_per_pair])

    return intra_edges, inter_edges


def plot_subgraph(
    subgraph: nx.Graph,
    weighted_degree: dict[str, float],
    output_path: Path,
    label_top_k: int,
    seed: int,
) -> None:
    community_membership = detect_communities(subgraph)
    positions = community_grouped_layout(subgraph, community_membership, seed=seed)
    bridge_scores = compute_bridge_scores(subgraph, community_membership)

    node_values = [weighted_degree.get(node, 0.0) for node in subgraph.nodes()]
    node_sizes = scale(node_values, 140, 1800)
    community_color_map = build_community_color_map(list(community_membership.values()))
    node_colors = [
        community_color_map[community_membership[node]]
        for node in subgraph.nodes()
    ]
    intra_edges, inter_edges = select_display_edges(
        subgraph,
        community_membership,
        intra_weight_threshold=3,
        inter_edges_per_pair=1,
    )
    intra_edge_widths = scale(
        [data.get("weight", 1) for _, _, data in intra_edges],
        0.5,
        2.2,
    )
    inter_edge_widths = scale(
        [data.get("weight", 1) for _, _, data in inter_edges],
        1.2,
        4.0,
    )
    bridge_threshold = sorted(bridge_scores.values(), reverse=True)[
        max(0, min(len(bridge_scores) - 1, max(2, len(bridge_scores) // 8) - 1))
    ] if bridge_scores else 0.0
    bridge_nodes = [
        node for node in subgraph.nodes()
        if bridge_scores.get(node, 0.0) >= bridge_threshold and bridge_scores.get(node, 0.0) > 0
    ]
    regular_nodes = [node for node in subgraph.nodes() if node not in bridge_nodes]
    node_size_map = dict(zip(subgraph.nodes(), node_sizes))

    plt.figure(figsize=(13.5, 8.5))
    nx.draw_networkx_edges(
        subgraph,
        positions,
        edgelist=[(left, right) for left, right, _ in intra_edges],
        width=intra_edge_widths,
        alpha=0.10,
        edge_color="#cbd5e1",
    )
    nx.draw_networkx_edges(
        subgraph,
        positions,
        edgelist=[(left, right) for left, right, _ in inter_edges],
        width=inter_edge_widths,
        alpha=0.55,
        edge_color="#334155",
    )
    nx.draw_networkx_nodes(
        subgraph,
        positions,
        nodelist=regular_nodes,
        node_size=[node_size_map[node] for node in regular_nodes],
        node_color=[community_color_map[community_membership[node]] for node in regular_nodes],
        linewidths=0.8,
        edgecolors="white",
        alpha=0.95,
    )
    if bridge_nodes:
        nx.draw_networkx_nodes(
            subgraph,
            positions,
            nodelist=bridge_nodes,
            node_size=[node_size_map[node] for node in bridge_nodes],
            node_color=[community_color_map[community_membership[node]] for node in bridge_nodes],
            linewidths=2.0,
            edgecolors="#111827",
            alpha=0.98,
        )

    label_nodes = choose_label_nodes(
        subgraph,
        community_membership,
        weighted_degree,
        label_top_k=label_top_k,
    )
    labels = {
        node: clean_author_name(subgraph.nodes[node].get("name", node))
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
    for text in plt.gca().texts:
        text.set_path_effects(
            [
                patheffects.Stroke(linewidth=3, foreground="white"),
                patheffects.Normal(),
            ]
        )

    plt.title(
        f"Coauthor Network Subgraph ({subgraph.number_of_nodes()} authors, "
        f"{subgraph.number_of_edges()} collaborations)",
        fontsize=16,
        pad=12,
    )
    legend_handles = [
        Patch(facecolor="#94a3b8", edgecolor="none", label="Community"),
        Line2D([0], [0], color="#cbd5e1", lw=2, label="Within-community collaboration"),
        Line2D([0], [0], color="#334155", lw=2.5, label="Cross-community backbone"),
        Line2D(
            [0],
            [0],
            marker="o",
            color="w",
            markerfacecolor="#9ca3af",
            markeredgecolor="#111827",
            markeredgewidth=2,
            markersize=10,
            label="Bridge author",
        ),
    ]
    plt.legend(
        handles=legend_handles,
        loc="upper right",
        frameon=True,
        framealpha=0.9,
        facecolor="white",
        edgecolor="#e5e7eb",
        fontsize=8,
    )
    plt.axis("off")
    plt.xlim(0.05, 0.95)
    plt.ylim(0.10, 0.88)
    plt.tight_layout(pad=0.4)
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
