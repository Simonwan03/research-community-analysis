#!/usr/bin/env python3
"""Compute betweenness centrality for a coauthor graph exported as CSV files."""

from __future__ import annotations

import argparse
import csv
import heapq
import json
import math
import random
from collections import defaultdict
from pathlib import Path
from statistics import mean

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute betweenness centrality on the coauthor graph defined by "
            "authors.csv and edges.csv."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025"),
        help="Directory containing author and edge CSVs.",
    )
    parser.add_argument(
        "--authors-csv",
        type=Path,
        default=None,
        help="Author CSV path. Defaults to <input-dir>/authors.csv.",
    )
    parser.add_argument(
        "--edges-csv",
        type=Path,
        default=None,
        help="Edge CSV path. Defaults to <input-dir>/edges.csv.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Output CSV path. Defaults to <input-dir>/results/<auto-name>.csv.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Output JSON summary path. Defaults to <input-dir>/results/<auto-name>_summary.json.",
    )
    parser.add_argument(
        "--distance-mode",
        choices=["unweighted", "inverse_weight"],
        default="inverse_weight",
        help=(
            "Shortest-path definition. 'unweighted' ignores edge weights. "
            "'inverse_weight' treats stronger collaboration edges as shorter distances via 1/weight."
        ),
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=512,
        help=(
            "Number of source nodes to sample for approximate betweenness. "
            "Use 0 or a value >= number of nodes to run exact Brandes. Default: 512"
        ),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed used when sampling source nodes. Default: 42",
    )
    parser.add_argument(
        "--normalized",
        action="store_true",
        help="Normalize scores to [0, 1] for an undirected graph.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=20,
        help="Number of top authors to print in the console summary. Default: 20",
    )
    return parser.parse_args()


def default_output_stem(authors_csv: Path, distance_mode: str) -> str:
    suffix = "" if distance_mode == "inverse_weight" else "_unweighted"
    if authors_csv.stem == "authors":
        return f"coauthor_betweenness{suffix}"
    return f"coauthor_betweenness_{authors_csv.stem.removeprefix('authors_')}{suffix}"


def load_authors(authors_csv: Path) -> pd.DataFrame:
    authors_df = pd.read_csv(authors_csv, low_memory=False).fillna("")
    if "author_id" not in authors_df.columns:
        raise ValueError(f"{authors_csv} is missing required column: author_id")
    authors_df["author_id"] = authors_df["author_id"].astype(str).str.strip()
    return authors_df.loc[authors_df["author_id"] != ""].copy()


def load_graph(
    authors_df: pd.DataFrame,
    edges_csv: Path,
) -> tuple[list[str], dict[str, list[tuple[str, float]]], dict[str, int], dict[str, int]]:
    nodes = authors_df["author_id"].tolist()
    node_set = set(nodes)
    adjacency_dict: dict[str, dict[str, float]] = {node: {} for node in nodes}
    degree: dict[str, int] = {node: 0 for node in nodes}
    weighted_degree: dict[str, int] = {node: 0 for node in nodes}

    with edges_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"source_author_id", "target_author_id", "weight"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{edges_csv} is missing required columns: {sorted(missing)}")

        for row in reader:
            source = str(row["source_author_id"]).strip()
            target = str(row["target_author_id"]).strip()
            if not source or not target or source == target:
                continue
            if source not in node_set:
                node_set.add(source)
                nodes.append(source)
                adjacency_dict[source] = {}
                degree[source] = 0
                weighted_degree[source] = 0
            if target not in node_set:
                node_set.add(target)
                nodes.append(target)
                adjacency_dict[target] = {}
                degree[target] = 0
                weighted_degree[target] = 0

            weight = int(float(row["weight"] or 0))
            if weight <= 0:
                continue

            is_new_edge = target not in adjacency_dict[source]
            adjacency_dict[source][target] = adjacency_dict[source].get(target, 0.0) + weight
            adjacency_dict[target][source] = adjacency_dict[target].get(source, 0.0) + weight
            weighted_degree[source] += weight
            weighted_degree[target] += weight
            if is_new_edge:
                degree[source] += 1
                degree[target] += 1

    adjacency = {
        node: sorted(neighbors.items(), key=lambda item: item[0])
        for node, neighbors in adjacency_dict.items()
    }
    return nodes, adjacency, degree, weighted_degree


def brandes_unweighted_source(
    source: str,
    adjacency: dict[str, list[tuple[str, float]]],
) -> dict[str, float]:
    stack: list[str] = []
    predecessors: dict[str, list[str]] = defaultdict(list)
    sigma: dict[str, float] = defaultdict(float)
    sigma[source] = 1.0
    distance: dict[str, int] = {source: 0}
    queue: list[str] = [source]
    head = 0

    while head < len(queue):
        v = queue[head]
        head += 1
        stack.append(v)
        for w, _ in adjacency[v]:
            if w not in distance:
                distance[w] = distance[v] + 1
                queue.append(w)
            if distance[w] == distance[v] + 1:
                sigma[w] += sigma[v]
                predecessors[w].append(v)

    dependency: dict[str, float] = defaultdict(float)
    contribution: dict[str, float] = {}
    while stack:
        w = stack.pop()
        if sigma[w] != 0.0:
            coeff = (1.0 + dependency[w]) / sigma[w]
            for v in predecessors[w]:
                dependency[v] += sigma[v] * coeff
        if w != source:
            contribution[w] = dependency[w]
    return contribution


def brandes_weighted_source(
    source: str,
    adjacency: dict[str, list[tuple[str, float]]],
) -> dict[str, float]:
    stack: list[str] = []
    predecessors: dict[str, list[str]] = defaultdict(list)
    sigma: dict[str, float] = defaultdict(float)
    sigma[source] = 1.0
    distance: dict[str, float] = {source: 0.0}
    visited: set[str] = set()
    heap: list[tuple[float, str]] = [(0.0, source)]
    epsilon = 1e-12

    while heap:
        dist_v, v = heapq.heappop(heap)
        if dist_v > distance.get(v, math.inf) + epsilon:
            continue
        if v in visited:
            continue
        visited.add(v)
        stack.append(v)

        for w, weight in adjacency[v]:
            edge_distance = 1.0 / float(weight)
            candidate = dist_v + edge_distance
            current = distance.get(w, math.inf)
            if candidate < current - epsilon:
                distance[w] = candidate
                sigma[w] = sigma[v]
                predecessors[w] = [v]
                heapq.heappush(heap, (candidate, w))
            elif abs(candidate - current) <= epsilon:
                sigma[w] += sigma[v]
                predecessors[w].append(v)

    dependency: dict[str, float] = defaultdict(float)
    contribution: dict[str, float] = {}
    while stack:
        w = stack.pop()
        if sigma[w] != 0.0:
            coeff = (1.0 + dependency[w]) / sigma[w]
            for v in predecessors[w]:
                dependency[v] += sigma[v] * coeff
        if w != source:
            contribution[w] = dependency[w]
    return contribution


def compute_betweenness(
    nodes: list[str],
    adjacency: dict[str, list[tuple[str, float]]],
    distance_mode: str,
    sample_size: int,
    seed: int,
    normalized: bool,
) -> tuple[dict[str, float], dict[str, object]]:
    num_nodes = len(nodes)
    if num_nodes == 0:
        return {}, {
            "mode": "exact",
            "sample_size": 0,
            "sampled_sources": 0,
            "normalized": normalized,
            "distance_mode": distance_mode,
            "scale_factor": 0.0,
        }

    exact = sample_size <= 0 or sample_size >= num_nodes
    if exact:
        sources = list(nodes)
        mode = "exact"
    else:
        rng = random.Random(seed)
        sources = rng.sample(nodes, sample_size)
        mode = "approximate"

    bc = {node: 0.0 for node in nodes}
    source_fn = (
        brandes_unweighted_source
        if distance_mode == "unweighted"
        else brandes_weighted_source
    )
    for index, source in enumerate(sources, start=1):
        contribution = source_fn(source, adjacency)
        for node, value in contribution.items():
            bc[node] += value
        if index % 50 == 0 or index == len(sources):
            print(f"[progress] processed_sources={index}/{len(sources)}")

    sample_scale = float(num_nodes) / float(len(sources)) if sources else 0.0
    if normalized:
        if num_nodes <= 2:
            final_scale = 0.0
        else:
            final_scale = sample_scale / ((num_nodes - 1) * (num_nodes - 2))
    else:
        final_scale = sample_scale * 0.5

    for node in bc:
        bc[node] *= final_scale

    metadata = {
        "mode": mode,
        "sample_size": sample_size,
        "sampled_sources": len(sources),
        "normalized": normalized,
        "distance_mode": distance_mode,
        "scale_factor": final_scale,
        "seed": seed,
    }
    return bc, metadata


def build_output(
    authors_df: pd.DataFrame,
    degree: dict[str, int],
    weighted_degree: dict[str, int],
    betweenness: dict[str, float],
) -> pd.DataFrame:
    output_df = authors_df.copy()
    output_df["degree"] = output_df["author_id"].map(lambda x: degree.get(x, 0))
    output_df["weighted_degree"] = output_df["author_id"].map(lambda x: weighted_degree.get(x, 0))
    output_df["betweenness_centrality"] = output_df["author_id"].map(
        lambda x: betweenness.get(x, 0.0)
    )
    output_df = output_df.sort_values(
        by=["betweenness_centrality", "weighted_degree", "paper_count", "name", "author_id"],
        ascending=[False, False, False, True, True],
    )
    return output_df


def build_summary(
    output_df: pd.DataFrame,
    authors_csv: Path,
    edges_csv: Path,
    run_metadata: dict[str, object],
    top_k: int,
) -> dict[str, object]:
    values = output_df["betweenness_centrality"].tolist()
    top_rows = output_df.head(top_k)
    return {
        "authors_csv": str(authors_csv),
        "edges_csv": str(edges_csv),
        "num_authors": int(len(output_df)),
        "num_connected_authors": int((output_df["degree"] > 0).sum()),
        "num_isolates": int((output_df["degree"] == 0).sum()),
        "betweenness_parameters": run_metadata,
        "betweenness_distribution": {
            "min": float(min(values)) if values else 0.0,
            "max": float(max(values)) if values else 0.0,
            "mean": float(mean(values)) if values else 0.0,
        },
        "top_authors": [
            {
                "rank": rank,
                "author_id": str(row.author_id),
                "name": str(getattr(row, "name", "")),
                "betweenness_centrality": float(row.betweenness_centrality),
                "degree": int(row.degree),
                "weighted_degree": int(row.weighted_degree),
                "paper_count": int(getattr(row, "paper_count", 0) or 0),
            }
            for rank, row in enumerate(top_rows.itertuples(index=False), start=1)
        ],
    }


def main() -> None:
    args = parse_args()
    authors_csv = args.authors_csv or (args.input_dir / "authors.csv")
    edges_csv = args.edges_csv or (args.input_dir / "edges.csv")
    stem = default_output_stem(authors_csv, args.distance_mode)
    results_dir = args.input_dir / "results"
    output_csv = args.output_csv or (results_dir / f"{stem}.csv")
    output_json = args.output_json or (results_dir / f"{stem}_summary.json")

    authors_df = load_authors(authors_csv)
    nodes, adjacency, degree, weighted_degree = load_graph(authors_df, edges_csv)
    betweenness, run_metadata = compute_betweenness(
        nodes=nodes,
        adjacency=adjacency,
        distance_mode=args.distance_mode,
        sample_size=args.sample_size,
        seed=args.seed,
        normalized=args.normalized,
    )
    output_df = build_output(
        authors_df=authors_df,
        degree=degree,
        weighted_degree=weighted_degree,
        betweenness=betweenness,
    )
    summary = build_summary(
        output_df=output_df,
        authors_csv=authors_csv,
        edges_csv=edges_csv,
        run_metadata=run_metadata,
        top_k=args.top_k,
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_csv, index=False, encoding="utf-8")
    output_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved betweenness CSV to {output_csv}")
    print(f"Saved betweenness summary JSON to {output_json}")
    print(
        "[betweenness] "
        f"authors={summary['num_authors']} | "
        f"connected_authors={summary['num_connected_authors']} | "
        f"isolates={summary['num_isolates']} | "
        f"mode={run_metadata['mode']} | "
        f"sampled_sources={run_metadata['sampled_sources']} | "
        f"distance_mode={run_metadata['distance_mode']} | "
        f"normalized={run_metadata['normalized']}"
    )
    print("[top]")
    for item in summary["top_authors"]:
        print(
            f"{item['rank']:>2}. "
            f"{item['name']} | betweenness={item['betweenness_centrality']:.8f} | "
            f"degree={item['degree']} | weighted_degree={item['weighted_degree']} | "
            f"paper_count={item['paper_count']}"
        )


if __name__ == "__main__":
    main()
