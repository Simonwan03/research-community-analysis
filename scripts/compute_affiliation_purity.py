#!/usr/bin/env python3
"""Compute community purity using all_affiliations as ground-truth labels."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute purity of detected communities using all_affiliations labels."
    )
    parser.add_argument(
        "--community-csv",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025/community_assignments_orcid_subgraph.csv"),
        help="CSV containing at least author_id and community_id.",
    )
    parser.add_argument(
        "--authors-csv",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025/authors_orcid_subgraph.csv"),
        help="CSV containing at least author_id and all_affiliations.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025/community_affiliation_purity.csv"),
        help="Output CSV path for per-community purity details.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025/community_affiliation_purity_summary.json"),
        help="Output JSON path for overall purity summary.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    communities_df = pd.read_csv(args.community_csv)
    authors_df = pd.read_csv(args.authors_csv, low_memory=False)

    required_community_cols = {"author_id", "community_id"}
    required_author_cols = {"author_id", "all_affiliations"}
    if not required_community_cols.issubset(communities_df.columns):
        raise SystemExit(
            f"Community CSV must contain columns: {', '.join(sorted(required_community_cols))}"
        )
    if not required_author_cols.issubset(authors_df.columns):
        raise SystemExit(
            f"Authors CSV must contain columns: {', '.join(sorted(required_author_cols))}"
        )

    authors_df = authors_df.copy()
    authors_df["all_affiliations"] = (
        authors_df["all_affiliations"].fillna("").astype(str).str.strip()
    )

    merged = communities_df.merge(
        authors_df[["author_id", "all_affiliations"]],
        on="author_id",
        how="inner",
    )
    merged = merged.loc[merged["all_affiliations"] != ""].copy()
    if merged.empty:
        raise SystemExit("No rows with non-empty all_affiliations labels were found after merging.")

    def split_affiliations(value: str) -> list[str]:
        return [part.strip() for part in value.split("|") if part.strip()]

    total_nodes = len(merged)
    rows = []
    purity_numerator = 0

    for community_id, group in merged.groupby("community_id", sort=True):
        community_size = int(len(group))
        affiliation_counts: dict[str, int] = {}
        for value in group["all_affiliations"]:
            labels = split_affiliations(str(value))
            unique_labels = set(labels)
            for label in unique_labels:
                affiliation_counts[label] = affiliation_counts.get(label, 0) + 1

        if not affiliation_counts:
            dominant_affiliation = ""
            dominant_count = 0
            distinct_affiliations = 0
        else:
            sorted_counts = sorted(
                affiliation_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )
            dominant_affiliation, dominant_count = sorted_counts[0]
            distinct_affiliations = len(affiliation_counts)

        community_purity = dominant_count / community_size if community_size else 0.0
        purity_numerator += dominant_count
        rows.append(
            {
                "community_id": int(community_id),
                "community_label": f"Community {int(community_id) + 1}",
                "community_size": community_size,
                "dominant_affiliation": dominant_affiliation,
                "dominant_affiliation_count": dominant_count,
                "community_purity": community_purity,
                "purity_contribution": dominant_count,
                "distinct_affiliations": distinct_affiliations,
            }
        )

    overall_purity = purity_numerator / total_nodes if total_nodes else 0.0
    result_df = pd.DataFrame(rows).sort_values(
        by=["community_id", "community_size"],
        ascending=[True, False],
    )
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(args.output_csv, index=False, encoding="utf-8")

    summary = {
        "ground_truth_label": "all_affiliations",
        "community_csv": str(args.community_csv),
        "authors_csv": str(args.authors_csv),
        "evaluated_nodes": total_nodes,
        "evaluated_communities": int(result_df.shape[0]),
        "purity_numerator": int(purity_numerator),
        "overall_purity": overall_purity,
    }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Saved per-community purity details to {args.output_csv}")
    print(f"Saved overall purity summary to {args.output_json}")
    print(
        "[purity] "
        f"evaluated_nodes={total_nodes} | "
        f"evaluated_communities={result_df.shape[0]} | "
        f"overall_purity={overall_purity:.6f}"
    )


if __name__ == "__main__":
    main()
