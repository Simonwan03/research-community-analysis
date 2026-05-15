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
        description="Visualize the current ORCID + affiliation coauthor subgraph."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025"),
        help="Directory containing authors_orcid_subgraph.csv and edges_orcid_subgraph.csv.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output PNG path. Defaults to <input-dir>/orcid_subgraph_top120.png.",
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
        default=3,
        help="Filter out edges lighter than this weight before plotting.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for spring layout.",
    )
    parser.add_argument(
        "--plot-full-graph",
        action="store_true",
        help="Plot the full filtered ORCID subgraph instead of selecting a top-k display subgraph.",
    )
    parser.add_argument(
        "--min-community-count",
        type=int,
        default=1,
        help="Drop communities smaller than this size before visualization. Default: 1",
    )
    return parser.parse_args()


def build_graph(input_dir: Path) -> tuple[nx.Graph, pd.DataFrame]:
    authors_path = input_dir / "authors_orcid_subgraph.csv"
    edges_path = input_dir / "edges_orcid_subgraph.csv"

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
            affiliation=(
                row.affiliation
                if hasattr(row, "affiliation") and isinstance(row.affiliation, str)
                else ""
            ),
            all_affiliations=(
                row.all_affiliations
                if hasattr(row, "all_affiliations") and isinstance(row.all_affiliations, str)
                else ""
            ),
        )

    for row in edges_df.itertuples(index=False):
        graph.add_edge(
            row.source_author_id,
            row.target_author_id,
            weight=int(row.weight),
            paper_count=int(row.paper_count),
        )

    return graph, authors_df


def load_precomputed_community_membership(input_dir: Path) -> dict[str, int]:
    community_path = input_dir / "community_assignments_orcid_subgraph.csv"
    if not community_path.exists():
        return {}

    community_df = pd.read_csv(community_path)
    required_columns = {"author_id", "community_id"}
    if not required_columns.issubset(community_df.columns):
        return {}

    return {
        str(row.author_id): int(row.community_id)
        for row in community_df.itertuples(index=False)
    }


def filter_graph_by_edge_weight(
    graph: nx.Graph,
    min_edge_weight: int,
) -> nx.Graph:
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


def select_subgraph(
    filtered_graph: nx.Graph,
    authors_df: pd.DataFrame,
    top_k: int,
    plot_full_graph: bool = False,
) -> tuple[nx.Graph, nx.Graph, dict[str, float]]:
    if filtered_graph.number_of_nodes() == 0:
        raise ValueError("Graph is empty after filtering. Try a lower --min-edge-weight.")

    weighted_degree = dict(filtered_graph.degree(weight="weight"))
    authors_df = authors_df.copy()
    authors_df["weighted_degree"] = authors_df["author_id"].map(weighted_degree).fillna(0)

    if plot_full_graph:
        return filtered_graph.copy(), filtered_graph, weighted_degree

    top_nodes = (
        authors_df.sort_values(
            by=["weighted_degree", "paper_count", "name"],
            ascending=[False, False, True],
        )
        .head(top_k)["author_id"]
        .tolist()
    )

    candidate_subgraph = filtered_graph.subgraph(top_nodes).copy()
    candidate_subgraph.remove_nodes_from(list(nx.isolates(candidate_subgraph)))

    if candidate_subgraph.number_of_nodes() == 0:
        raise ValueError(
            "Subgraph is empty after selecting top-k authors. "
            "Try a larger --top-k or lower --min-edge-weight."
        )

    largest_component_nodes = max(nx.connected_components(candidate_subgraph), key=len)
    display_subgraph = candidate_subgraph.subgraph(largest_component_nodes).copy()

    return display_subgraph, filtered_graph, weighted_degree


def filter_small_communities(
    graph: nx.Graph,
    community_membership: dict[str, int],
    min_community_count: int,
) -> tuple[nx.Graph, dict[str, int]]:
    if min_community_count <= 1:
        return graph, community_membership

    community_sizes: dict[int, int] = {}
    for node in graph.nodes():
        community_id = community_membership[node]
        community_sizes[community_id] = community_sizes.get(community_id, 0) + 1

    keep_nodes = {
        node
        for node in graph.nodes()
        if community_sizes.get(community_membership[node], 0) >= min_community_count
    }
    filtered_graph = graph.subgraph(keep_nodes).copy()
    filtered_graph.remove_nodes_from(list(nx.isolates(filtered_graph)))
    filtered_membership = {
        node: community_membership[node]
        for node in filtered_graph.nodes()
    }
    return filtered_graph, filtered_membership


def detect_communities(subgraph: nx.Graph) -> dict[str, int]:
    if subgraph.number_of_nodes() <= 1:
        return {node: 0 for node in subgraph.nodes()}

    communities = list(
        nx.community.louvain_communities(
            subgraph,
            weight="weight",
            seed=42,
        )
    )
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
        extra_needed = len(unique_ids) - len(base_palette)
        generated_colors = [
            mcolors.to_hex(mcolors.hsv_to_rgb((index / max(extra_needed, 1), 0.55, 0.9)))
            for index in range(extra_needed)
        ]
        base_palette.extend(generated_colors)
    return {
        community_id: base_palette[index]
        for index, community_id in enumerate(unique_ids)
    }


def save_community_assignments(
    output_path: Path,
    full_graph: nx.Graph,
    subgraph: nx.Graph,
    full_community_membership: dict[str, int],
    weighted_degree: dict[str, float],
    bridge_scores: dict[str, float],
) -> None:
    rows = []
    subgraph_nodes = set(subgraph.nodes())
    for node in sorted(
        full_graph.nodes(),
        key=lambda item: (
            weighted_degree.get(item, 0.0),
            full_graph.nodes[item].get("paper_count", 0),
            clean_author_name(full_graph.nodes[item].get("name", item)),
        ),
        reverse=True,
    ):
        rows.append(
            {
                "author_id": node,
                "name": full_graph.nodes[node].get("name", node),
                "display_name": clean_author_name(full_graph.nodes[node].get("name", node)),
                "dblp_pid": full_graph.nodes[node].get("dblp_pid", ""),
                "orcid": full_graph.nodes[node].get("orcid", ""),
                "affiliation": full_graph.nodes[node].get("affiliation", ""),
                "all_affiliations": full_graph.nodes[node].get("all_affiliations", ""),
                "community_id": full_community_membership[node],
                "community_label": f"Community {full_community_membership[node] + 1}",
                "weighted_degree": int(weighted_degree.get(node, 0.0)),
                "paper_count": int(full_graph.nodes[node].get("paper_count", 0)),
                "bridge_score": float(bridge_scores.get(node, 0.0)),
                "in_visualized_subgraph": int(node in subgraph_nodes),
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_path, index=False, encoding="utf-8")


def choose_label_nodes(
    subgraph: nx.Graph,
    community_membership: dict[str, int],
    weighted_degree: dict[str, float],
    label_top_k: int,
    include_community_representatives: bool = True,
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

    if not include_community_representatives:
        return sorted(
            selected,
            key=lambda node: (
                weighted_degree.get(node, 0.0),
                subgraph.nodes[node].get("paper_count", 0),
                clean_author_name(subgraph.nodes[node].get("name", node)),
            ),
            reverse=True,
        )

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
    full_community_membership: dict[str, int],
    weighted_degree: dict[str, float],
    full_bridge_scores: dict[str, float],
    output_path: Path,
    label_top_k: int,
    seed: int,
    plot_full_graph: bool = False,
) -> None:
    community_membership = {
        node: full_community_membership[node]
        for node in subgraph.nodes()
    }
    positions = community_grouped_layout(subgraph, community_membership, seed=seed)
    bridge_scores = {
        node: full_bridge_scores.get(node, 0.0)
        for node in subgraph.nodes()
    }

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
        include_community_representatives=not plot_full_graph,
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
        (
            "Coauthor Network Full Graph"
            if plot_full_graph
            else "Coauthor Network Subgraph"
        )
        + f" ({subgraph.number_of_nodes()} authors, {subgraph.number_of_edges()} collaborations)",
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
    output_path = args.output or (
        args.input_dir / (
            "orcid_subgraph_full.png"
            if args.plot_full_graph
            else f"orcid_subgraph_top{args.top_k}.png"
        )
    )
    community_output_path = args.input_dir / "community_assignments_orcid_subgraph_visualization.csv"

    graph, authors_df = build_graph(args.input_dir)
    filtered_graph = filter_graph_by_edge_weight(graph, args.min_edge_weight)
    precomputed_membership = load_precomputed_community_membership(args.input_dir)
    if precomputed_membership and all(node in precomputed_membership for node in filtered_graph.nodes()):
        full_community_membership = {
            node: precomputed_membership[node]
            for node in filtered_graph.nodes()
        }
    else:
        full_community_membership = detect_communities(filtered_graph)
    filtered_graph, full_community_membership = filter_small_communities(
        filtered_graph,
        full_community_membership,
        min_community_count=args.min_community_count,
    )
    subgraph, filtered_graph, weighted_degree = select_subgraph(
        filtered_graph,
        authors_df,
        top_k=args.top_k,
        plot_full_graph=args.plot_full_graph,
    )
    full_bridge_scores = compute_bridge_scores(filtered_graph, full_community_membership)
    save_community_assignments(
        community_output_path,
        filtered_graph,
        subgraph,
        full_community_membership,
        weighted_degree,
        full_bridge_scores,
    )
    plot_subgraph(
        subgraph,
        full_community_membership,
        weighted_degree,
        full_bridge_scores,
        output_path=output_path,
        label_top_k=args.label_top_k,
        seed=args.seed,
        plot_full_graph=args.plot_full_graph,
    )

    print(
        f"Saved visualization to {output_path} "
        f"with {subgraph.number_of_nodes()} nodes and {subgraph.number_of_edges()} edges. "
        f"Saved community assignments to {community_output_path}."
    )


if __name__ == "__main__":
    main()
