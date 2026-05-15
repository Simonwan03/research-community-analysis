#!/usr/bin/env python3
"""Compute eigenvector centrality for a coauthor graph exported as CSV files."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from statistics import mean

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute weighted eigenvector centrality on the coauthor graph defined "
            "by authors.csv and edges.csv."
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
        "--tolerance",
        type=float,
        default=1e-10,
        help="Convergence tolerance on max absolute score change. Default: 1e-10",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=500,
        help="Maximum power-iteration steps. Default: 500",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=20,
        help="Number of top authors to print in the console summary. Default: 20",
    )
    return parser.parse_args()


def default_output_stem(authors_csv: Path, edges_csv: Path) -> str:
    if authors_csv.stem == "authors" and edges_csv.stem == "edges":
        return "coauthor_eigenvector"
    return f"coauthor_eigenvector_{authors_csv.stem.removeprefix('authors_')}"


def load_authors(authors_csv: Path) -> pd.DataFrame:
    authors_df = pd.read_csv(authors_csv, low_memory=False).fillna("")
    if "author_id" not in authors_df.columns:
        raise ValueError(f"{authors_csv} is missing required column: author_id")
    authors_df["author_id"] = authors_df["author_id"].astype(str).str.strip()
    return authors_df.loc[authors_df["author_id"] != ""].copy()


def load_graph(
    authors_df: pd.DataFrame,
    edges_csv: Path,
) -> tuple[list[str], dict[str, dict[str, float]], dict[str, int], dict[str, int]]:
    nodes = authors_df["author_id"].tolist()
    adjacency: dict[str, dict[str, float]] = {node: {} for node in nodes}
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

            if source not in adjacency:
                adjacency[source] = {}
                degree[source] = 0
                weighted_degree[source] = 0
                nodes.append(source)
            if target not in adjacency:
                adjacency[target] = {}
                degree[target] = 0
                weighted_degree[target] = 0
                nodes.append(target)

            weight = int(float(row["weight"] or 0))
            if weight <= 0:
                continue

            is_new_edge = target not in adjacency[source]
            adjacency[source][target] = adjacency[source].get(target, 0.0) + weight
            adjacency[target][source] = adjacency[target].get(source, 0.0) + weight
            weighted_degree[source] += weight
            weighted_degree[target] += weight
            if is_new_edge:
                degree[source] += 1
                degree[target] += 1

    return nodes, adjacency, degree, weighted_degree


def l2_normalize(scores: dict[str, float]) -> dict[str, float]:
    norm = math.sqrt(sum(value * value for value in scores.values()))
    if norm == 0.0:
        return {node: 0.0 for node in scores}
    return {node: value / norm for node, value in scores.items()}


def rayleigh_quotient(
    scores: dict[str, float],
    adjacency: dict[str, dict[str, float]],
) -> float:
    numerator = 0.0
    denominator = sum(value * value for value in scores.values())
    if denominator == 0.0:
        return 0.0
    for node, neighbors in adjacency.items():
        numerator += scores[node] * sum(weight * scores[neighbor] for neighbor, weight in neighbors.items())
    return numerator / denominator


def compute_eigenvector_centrality(
    nodes: list[str],
    adjacency: dict[str, dict[str, float]],
    tolerance: float,
    max_iterations: int,
) -> tuple[dict[str, float], int, float, float]:
    num_nodes = len(nodes)
    if num_nodes == 0:
        return {}, 0, 0.0, 0.0

    scores = {node: 1.0 for node in nodes}
    scores = l2_normalize(scores)
    final_delta = 0.0

    for iteration in range(1, max_iterations + 1):
        next_scores: dict[str, float] = {}
        for node in nodes:
            # Use (A + I) x instead of A x to avoid oscillation on nearly bipartite structure.
            value = scores[node]
            for neighbor, weight in adjacency[node].items():
                value += weight * scores[neighbor]
            next_scores[node] = value

        next_scores = l2_normalize(next_scores)
        final_delta = max(abs(next_scores[node] - scores[node]) for node in nodes)
        scores = next_scores
        if final_delta <= tolerance:
            eigenvalue = rayleigh_quotient(scores, adjacency)
            return scores, iteration, final_delta, eigenvalue

    eigenvalue = rayleigh_quotient(scores, adjacency)
    return scores, max_iterations, final_delta, eigenvalue


def build_output(
    authors_df: pd.DataFrame,
    degree: dict[str, int],
    weighted_degree: dict[str, int],
    eigenvector_scores: dict[str, float],
) -> pd.DataFrame:
    output_df = authors_df.copy()
    output_df["degree"] = output_df["author_id"].map(lambda x: degree.get(x, 0))
    output_df["weighted_degree"] = output_df["author_id"].map(lambda x: weighted_degree.get(x, 0))
    output_df["eigenvector_centrality"] = output_df["author_id"].map(
        lambda x: eigenvector_scores.get(x, 0.0)
    )
    output_df = output_df.sort_values(
        by=["eigenvector_centrality", "weighted_degree", "paper_count", "name", "author_id"],
        ascending=[False, False, False, True, True],
    )
    return output_df


def build_summary(
    output_df: pd.DataFrame,
    authors_csv: Path,
    edges_csv: Path,
    tolerance: float,
    max_iterations: int,
    iterations_run: int,
    final_delta: float,
    dominant_eigenvalue: float,
    top_k: int,
) -> dict[str, object]:
    values = output_df["eigenvector_centrality"].tolist()
    top_rows = output_df.head(top_k)
    return {
        "authors_csv": str(authors_csv),
        "edges_csv": str(edges_csv),
        "num_authors": int(len(output_df)),
        "num_connected_authors": int((output_df["degree"] > 0).sum()),
        "num_isolates": int((output_df["degree"] == 0).sum()),
        "eigenvector_parameters": {
            "tolerance": tolerance,
            "max_iterations": max_iterations,
            "iterations_run": iterations_run,
            "final_max_delta": final_delta,
            "dominant_eigenvalue_estimate": dominant_eigenvalue,
        },
        "eigenvector_distribution": {
            "min": float(min(values)) if values else 0.0,
            "max": float(max(values)) if values else 0.0,
            "mean": float(mean(values)) if values else 0.0,
            "l2_norm": float(math.sqrt(sum(value * value for value in values))) if values else 0.0,
        },
        "top_authors": [
            {
                "rank": rank,
                "author_id": str(row.author_id),
                "name": str(getattr(row, "name", "")),
                "eigenvector_centrality": float(row.eigenvector_centrality),
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
    stem = default_output_stem(authors_csv, edges_csv)
    results_dir = args.input_dir / "results"
    output_csv = args.output_csv or (results_dir / f"{stem}.csv")
    output_json = args.output_json or (results_dir / f"{stem}_summary.json")

    authors_df = load_authors(authors_csv)
    nodes, adjacency, degree, weighted_degree = load_graph(authors_df, edges_csv)
    scores, iterations_run, final_delta, dominant_eigenvalue = compute_eigenvector_centrality(
        nodes=nodes,
        adjacency=adjacency,
        tolerance=args.tolerance,
        max_iterations=args.max_iterations,
    )
    output_df = build_output(
        authors_df=authors_df,
        degree=degree,
        weighted_degree=weighted_degree,
        eigenvector_scores=scores,
    )
    summary = build_summary(
        output_df=output_df,
        authors_csv=authors_csv,
        edges_csv=edges_csv,
        tolerance=args.tolerance,
        max_iterations=args.max_iterations,
        iterations_run=iterations_run,
        final_delta=final_delta,
        dominant_eigenvalue=dominant_eigenvalue,
        top_k=args.top_k,
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_csv, index=False, encoding="utf-8")
    output_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved eigenvector centrality CSV to {output_csv}")
    print(f"Saved eigenvector centrality summary JSON to {output_json}")
    print(
        "[eigenvector] "
        f"authors={summary['num_authors']} | "
        f"connected_authors={summary['num_connected_authors']} | "
        f"isolates={summary['num_isolates']} | "
        f"iterations={iterations_run} | "
        f"final_max_delta={final_delta:.3e} | "
        f"dominant_eigenvalue≈{dominant_eigenvalue:.6f}"
    )
    print("[top]")
    for item in summary["top_authors"]:
        print(
            f"{item['rank']:>2}. "
            f"{item['name']} | eigenvector={item['eigenvector_centrality']:.8f} | "
            f"degree={item['degree']} | weighted_degree={item['weighted_degree']} | "
            f"paper_count={item['paper_count']}"
        )


if __name__ == "__main__":
    main()
