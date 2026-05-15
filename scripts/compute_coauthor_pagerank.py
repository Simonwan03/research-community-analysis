#!/usr/bin/env python3
"""Compute PageRank for a coauthor graph exported as authors/edges CSV files."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import mean

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compute weighted PageRank on the coauthor graph defined by authors.csv "
            "and edges.csv."
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
        "--damping",
        type=float,
        default=0.85,
        help="PageRank damping factor. Default: 0.85",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=1e-10,
        help="Convergence tolerance on L1 delta. Default: 1e-10",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=200,
        help="Maximum PageRank iterations. Default: 200",
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
        return "coauthor_pagerank"
    return f"coauthor_pagerank_{authors_csv.stem.removeprefix('authors_')}"


def load_authors(authors_csv: Path) -> pd.DataFrame:
    authors_df = pd.read_csv(authors_csv, low_memory=False).fillna("")
    if "author_id" not in authors_df.columns:
        raise ValueError(f"{authors_csv} is missing required column: author_id")
    authors_df["author_id"] = authors_df["author_id"].astype(str).str.strip()
    authors_df = authors_df.loc[authors_df["author_id"] != ""].copy()
    return authors_df


def load_graph(
    authors_df: pd.DataFrame,
    edges_csv: Path,
) -> tuple[list[str], dict[str, dict[str, float]], dict[str, float], dict[str, int]]:
    nodes = authors_df["author_id"].tolist()
    adjacency: dict[str, dict[str, float]] = {node: {} for node in nodes}
    degree: dict[str, int] = {node: 0 for node in nodes}
    weighted_degree: dict[str, float] = {node: 0.0 for node in nodes}

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
                weighted_degree[source] = 0.0
                nodes.append(source)
            if target not in adjacency:
                adjacency[target] = {}
                degree[target] = 0
                weighted_degree[target] = 0.0
                nodes.append(target)

            weight = float(row["weight"] or 0.0)
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

    return nodes, adjacency, weighted_degree, degree


def compute_pagerank(
    nodes: list[str],
    adjacency: dict[str, dict[str, float]],
    damping: float,
    tolerance: float,
    max_iterations: int,
) -> tuple[dict[str, float], int, float]:
    num_nodes = len(nodes)
    if num_nodes == 0:
        return {}, 0, 0.0

    scores = {node: 1.0 / num_nodes for node in nodes}
    outgoing_weight = {
        node: sum(neighbors.values())
        for node, neighbors in adjacency.items()
    }

    final_delta = 0.0
    for iteration in range(1, max_iterations + 1):
        dangling_mass = sum(
            scores[node]
            for node in nodes
            if outgoing_weight.get(node, 0.0) == 0.0
        )
        base_score = (1.0 - damping) / num_nodes + damping * dangling_mass / num_nodes
        next_scores = {node: base_score for node in nodes}

        for node in nodes:
            total_weight = outgoing_weight.get(node, 0.0)
            if total_weight == 0.0:
                continue
            contribution_scale = damping * scores[node] / total_weight
            for neighbor, weight in adjacency[node].items():
                next_scores[neighbor] += contribution_scale * weight

        final_delta = sum(abs(next_scores[node] - scores[node]) for node in nodes)
        scores = next_scores
        if final_delta <= tolerance:
            return scores, iteration, final_delta

    return scores, max_iterations, final_delta


def build_output(
    authors_df: pd.DataFrame,
    degree: dict[str, int],
    weighted_degree: dict[str, float],
    pagerank: dict[str, float],
) -> pd.DataFrame:
    output_df = authors_df.copy()
    output_df["degree"] = output_df["author_id"].map(lambda x: degree.get(x, 0))
    output_df["weighted_degree"] = output_df["author_id"].map(
        lambda x: int(round(weighted_degree.get(x, 0.0)))
    )
    output_df["pagerank"] = output_df["author_id"].map(lambda x: pagerank.get(x, 0.0))
    output_df = output_df.sort_values(
        by=["pagerank", "weighted_degree", "paper_count", "name", "author_id"],
        ascending=[False, False, False, True, True],
    )
    return output_df


def build_summary(
    output_df: pd.DataFrame,
    authors_csv: Path,
    edges_csv: Path,
    damping: float,
    tolerance: float,
    max_iterations: int,
    iterations_run: int,
    final_delta: float,
    top_k: int,
) -> dict[str, object]:
    top_rows = output_df.head(top_k)
    pagerank_values = output_df["pagerank"].tolist()
    return {
        "authors_csv": str(authors_csv),
        "edges_csv": str(edges_csv),
        "num_authors": int(len(output_df)),
        "num_connected_authors": int((output_df["degree"] > 0).sum()),
        "num_isolates": int((output_df["degree"] == 0).sum()),
        "pagerank_parameters": {
            "damping": damping,
            "tolerance": tolerance,
            "max_iterations": max_iterations,
            "iterations_run": iterations_run,
            "final_l1_delta": final_delta,
        },
        "pagerank_distribution": {
            "min": float(min(pagerank_values)) if pagerank_values else 0.0,
            "max": float(max(pagerank_values)) if pagerank_values else 0.0,
            "mean": float(mean(pagerank_values)) if pagerank_values else 0.0,
        },
        "top_authors": [
            {
                "rank": rank,
                "author_id": str(row.author_id),
                "name": str(getattr(row, "name", "")),
                "pagerank": float(row.pagerank),
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
    nodes, adjacency, weighted_degree, degree = load_graph(authors_df, edges_csv)
    pagerank, iterations_run, final_delta = compute_pagerank(
        nodes=nodes,
        adjacency=adjacency,
        damping=args.damping,
        tolerance=args.tolerance,
        max_iterations=args.max_iterations,
    )
    output_df = build_output(
        authors_df=authors_df,
        degree=degree,
        weighted_degree=weighted_degree,
        pagerank=pagerank,
    )
    summary = build_summary(
        output_df=output_df,
        authors_csv=authors_csv,
        edges_csv=edges_csv,
        damping=args.damping,
        tolerance=args.tolerance,
        max_iterations=args.max_iterations,
        iterations_run=iterations_run,
        final_delta=final_delta,
        top_k=args.top_k,
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_csv, index=False, encoding="utf-8")
    output_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved PageRank CSV to {output_csv}")
    print(f"Saved PageRank summary JSON to {output_json}")
    print(
        "[pagerank] "
        f"authors={summary['num_authors']} | "
        f"connected_authors={summary['num_connected_authors']} | "
        f"isolates={summary['num_isolates']} | "
        f"iterations={iterations_run} | "
        f"final_l1_delta={final_delta:.3e}"
    )
    print("[top]")
    for item in summary["top_authors"]:
        print(
            f"{item['rank']:>2}. "
            f"{item['name']} | pagerank={item['pagerank']:.8f} | "
            f"degree={item['degree']} | weighted_degree={item['weighted_degree']} | "
            f"paper_count={item['paper_count']}"
        )


if __name__ == "__main__":
    main()
