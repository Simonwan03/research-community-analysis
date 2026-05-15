#!/usr/bin/env python3
"""Backfill ORCID from a local DBLP dump, then build an ORCID-only subgraph and compute communities."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import time

import networkx as nx
import pandas as pd

from backfill_orcid_from_pid import lookup_orcids_from_local_dump
from enrich_top_authors_profiles import fetch_json, lookup_openalex_author
from manual_affiliation_country_overrides import MANUAL_AFFILIATION_COUNTRY_OVERRIDES
from visualize_coauthor_graph import compute_bridge_scores, detect_communities

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill ORCID for all authors from a local DBLP dump, keep only authors with "
            "ORCID, and compute communities on the resulting subgraph."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025"),
        help="Directory containing authors.csv and edges.csv.",
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
        "--dblp-xml-gz",
        type=Path,
        default=Path("dblp_data/dblp.xml.gz"),
        help="Local DBLP dump used to backfill ORCID.",
    )
    parser.add_argument(
        "--min-edge-weight",
        type=int,
        default=3,
        help="Keep only edges with weight >= this threshold in the ORCID subgraph.",
    )
    parser.add_argument(
        "--backfilled-authors-output",
        type=Path,
        default=None,
        help="Output CSV for all authors after local ORCID backfill. Defaults to <input-dir>/authors_orcid_backfilled.csv.",
    )
    parser.add_argument(
        "--orcid-authors-output",
        type=Path,
        default=None,
        help="Output CSV for authors retained in the ORCID-only subgraph. Defaults to <input-dir>/authors_orcid_subgraph.csv.",
    )
    parser.add_argument(
        "--orcid-edges-output",
        type=Path,
        default=None,
        help="Output CSV for edges retained in the ORCID-only subgraph. Defaults to <input-dir>/edges_orcid_subgraph.csv.",
    )
    parser.add_argument(
        "--community-output",
        type=Path,
        default=None,
        help="Output CSV for community assignments on the ORCID-only subgraph. Defaults to <input-dir>/community_assignments_orcid_subgraph.csv.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.5,
        help="Delay between OpenAlex affiliation requests. Default: 0.5",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=60.0,
        help="HTTP timeout for each OpenAlex request. Default: 60.0",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum retries for transient OpenAlex request failures. Default: 5",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Maximum concurrent OpenAlex requests. Default: 8",
    )
    parser.add_argument(
        "--local-orcid-cache",
        type=Path,
        default=None,
        help="Cache file for local pid->ORCID results. Defaults to <input-dir>/local_pid_orcid_cache.csv.",
    )
    parser.add_argument(
        "--ror-cache",
        type=Path,
        default=None,
        help="Cache file for affiliation->ROR country resolution. Defaults to <input-dir>/ror_affiliation_cache.csv.",
    )
    return parser.parse_args()


def build_graph_from_frames(authors_df: pd.DataFrame, edges_df: pd.DataFrame) -> nx.Graph:
    graph = nx.Graph()
    for row in authors_df.itertuples(index=False):
        graph.add_node(
            row.author_id,
            name=row.name,
            paper_count=int(row.paper_count),
            dblp_pid=row.dblp_pid if isinstance(row.dblp_pid, str) else "",
            orcid=row.orcid if isinstance(row.orcid, str) else "",
        )

    for row in edges_df.itertuples(index=False):
        graph.add_edge(
            row.source_author_id,
            row.target_author_id,
            weight=int(row.weight),
            paper_count=int(row.paper_count),
        )
    return graph


def load_local_orcid_cache(cache_path: Path) -> dict[str, str]:
    if not cache_path.exists():
        return {}
    cache_df = pd.read_csv(cache_path, dtype=str).fillna("")
    if "dblp_pid" not in cache_df.columns or "orcid" not in cache_df.columns:
        return {}
    return {
        str(row["dblp_pid"]).strip(): str(row["orcid"]).strip()
        for _, row in cache_df.iterrows()
        if str(row["dblp_pid"]).strip()
    }


def save_local_orcid_cache(cache_path: Path, pid_to_orcid: dict[str, str]) -> None:
    cache_rows = [
        {"dblp_pid": pid, "orcid": orcid}
        for pid, orcid in sorted(pid_to_orcid.items())
    ]
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(cache_rows).to_csv(cache_path, index=False, encoding="utf-8")


def split_affiliations(text: str) -> list[str]:
    return [part.strip() for part in str(text).split("|") if part.strip()]


def load_ror_cache(cache_path: Path) -> dict[str, dict[str, str]]:
    if not cache_path.exists():
        return {}
    cache_df = pd.read_csv(cache_path, dtype=str).fillna("")
    if "affiliation_label" not in cache_df.columns:
        return {}
    result: dict[str, dict[str, str]] = {}
    for row in cache_df.to_dict(orient="records"):
        label = str(row.get("affiliation_label", "")).strip()
        if not label:
            continue
        result[label] = {
            "ror_id": str(row.get("ror_id", "")).strip(),
            "ror_name": str(row.get("ror_name", "")).strip(),
            "ror_country_code": str(row.get("ror_country_code", "")).strip(),
            "ror_country_name": str(row.get("ror_country_name", "")).strip(),
            "ror_match_status": str(row.get("ror_match_status", "")).strip(),
        }
    return result


def save_ror_cache(cache_path: Path, cache: dict[str, dict[str, str]]) -> None:
    rows = []
    for label in sorted(cache):
        entry = cache[label]
        rows.append(
            {
                "affiliation_label": label,
                "ror_id": entry.get("ror_id", ""),
                "ror_name": entry.get("ror_name", ""),
                "ror_country_code": entry.get("ror_country_code", ""),
                "ror_country_name": entry.get("ror_country_name", ""),
                "ror_match_status": entry.get("ror_match_status", ""),
            }
        )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(cache_path, index=False, encoding="utf-8")


def lookup_ror_affiliation(
    affiliation_label: str,
    timeout_seconds: float,
    max_retries: int,
) -> dict[str, str]:
    if affiliation_label in MANUAL_AFFILIATION_COUNTRY_OVERRIDES:
        manual = MANUAL_AFFILIATION_COUNTRY_OVERRIDES[affiliation_label]
        return {
            "ror_id": manual.get("ror_id", ""),
            "ror_name": manual.get("ror_name", ""),
            "ror_country_code": manual.get("country_code", ""),
            "ror_country_name": manual.get("country_name", ""),
            "ror_match_status": manual.get("match_status", "manual"),
        }

    payload = fetch_json(
        "https://api.ror.org/v2/organizations",
        {"affiliation": affiliation_label},
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
    items = payload.get("items") or []
    if not items:
        return {
            "ror_id": "",
            "ror_name": "",
            "ror_country_code": "",
            "ror_country_name": "",
            "ror_match_status": "not_found",
        }

    item = items[0]
    organization = item.get("organization") or {}
    locations = organization.get("locations") or []
    geonames = {}
    if locations and isinstance(locations[0], dict):
        geonames = locations[0].get("geonames_details") or {}

    name = organization.get("name", "")
    if not name:
        names = organization.get("names") or []
        if names and isinstance(names[0], dict):
            name = names[0].get("value", "")

    return {
        "ror_id": str(organization.get("id", "")).strip(),
        "ror_name": str(name).strip(),
        "ror_country_code": str(geonames.get("country_code", "")).strip(),
        "ror_country_name": str(geonames.get("country_name", "")).strip(),
        "ror_match_status": "matched",
    }


def enrich_ror_countries(
    authors_df: pd.DataFrame,
    cache_path: Path,
    sleep_seconds: float,
    timeout_seconds: float,
    max_retries: int,
) -> pd.DataFrame:
    authors_df = authors_df.copy()
    authors_df["ror_id"] = ""
    authors_df["ror_name"] = ""
    authors_df["ror_country_code"] = ""
    authors_df["ror_country_name"] = ""
    authors_df["ror_match_status"] = ""
    authors_df["all_affiliations_ror_ids"] = ""
    authors_df["all_affiliations_country_codes"] = ""
    authors_df["all_affiliations_country_names"] = ""

    ror_cache = load_ror_cache(cache_path)
    unique_labels: set[str] = set()
    for row in authors_df.itertuples(index=False):
        unique_labels.update(split_affiliations(getattr(row, "all_affiliations", "")))
        primary = str(getattr(row, "affiliation", "")).strip()
        if primary:
            unique_labels.add(primary)

    labels_to_query = [label for label in sorted(unique_labels) if label not in ror_cache]
    progress = tqdm(total=len(labels_to_query), desc="ROR affiliation->country", unit="affiliation") if tqdm is not None else None
    try:
        for label in labels_to_query:
            try:
                ror_cache[label] = lookup_ror_affiliation(
                    label,
                    timeout_seconds=timeout_seconds,
                    max_retries=max_retries,
                )
            except Exception as exc:
                ror_cache[label] = {
                    "ror_id": "",
                    "ror_name": "",
                    "ror_country_code": "",
                    "ror_country_name": "",
                    "ror_match_status": f"error:{type(exc).__name__}",
                }
            if progress is not None:
                progress.update(1)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
    finally:
        if progress is not None:
            progress.close()

    save_ror_cache(cache_path, ror_cache)

    for index, row in authors_df.iterrows():
        primary = str(row.get("affiliation", "")).strip()
        primary_entry = ror_cache.get(primary, {}) if primary else {}
        authors_df.at[index, "ror_id"] = primary_entry.get("ror_id", "")
        authors_df.at[index, "ror_name"] = primary_entry.get("ror_name", "")
        authors_df.at[index, "ror_country_code"] = primary_entry.get("ror_country_code", "")
        authors_df.at[index, "ror_country_name"] = primary_entry.get("ror_country_name", "")
        authors_df.at[index, "ror_match_status"] = primary_entry.get("ror_match_status", "")

        labels = split_affiliations(str(row.get("all_affiliations", "")))
        authors_df.at[index, "all_affiliations_ror_ids"] = "|".join(
            ror_cache.get(label, {}).get("ror_id", "") for label in labels
        )
        authors_df.at[index, "all_affiliations_country_codes"] = "|".join(
            ror_cache.get(label, {}).get("ror_country_code", "") for label in labels
        )
        authors_df.at[index, "all_affiliations_country_names"] = "|".join(
            ror_cache.get(label, {}).get("ror_country_name", "") for label in labels
        )

    return authors_df


def enrich_affiliations(
    authors_df: pd.DataFrame,
    sleep_seconds: float,
    timeout_seconds: float,
    max_retries: int,
    max_workers: int,
) -> pd.DataFrame:
    authors_df = authors_df.copy()
    authors_df["openalex_id"] = ""
    authors_df["openalex_display_name"] = ""
    authors_df["affiliation"] = ""
    authors_df["all_affiliations"] = ""
    authors_df["affiliation_country_code"] = ""
    authors_df["affiliation_source"] = ""
    authors_df["openalex_match_status"] = ""

    def fetch_one(index: int) -> tuple[int, dict[str, str]]:
        import time

        name = str(authors_df.at[index, "name"])
        orcid = str(authors_df.at[index, "orcid"])
        try:
            fields = lookup_openalex_author(
                orcid,
                name,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
            )
        except Exception as exc:
            fields = {
                "openalex_id": "",
                "openalex_display_name": "",
                "employer": "",
                "all_employers": "",
                "employer_country_code": "",
                "employer_source": "error",
                "openalex_match_status": f"error:{type(exc).__name__}",
            }

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        return index, fields

    indices = list(authors_df.index)
    progress = tqdm(total=len(indices), desc="OpenAlex ORCID->affiliation", unit="author") if tqdm is not None else None

    try:
        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
            futures = [executor.submit(fetch_one, index) for index in indices]
            for future in as_completed(futures):
                index, fields = future.result()
                authors_df.at[index, "openalex_id"] = fields.get("openalex_id", "")
                authors_df.at[index, "openalex_display_name"] = fields.get("openalex_display_name", "")
                authors_df.at[index, "affiliation"] = fields.get("employer", "")
                authors_df.at[index, "all_affiliations"] = fields.get("all_employers", "")
                authors_df.at[index, "affiliation_country_code"] = fields.get("employer_country_code", "")
                authors_df.at[index, "affiliation_source"] = fields.get("employer_source", "")
                authors_df.at[index, "openalex_match_status"] = fields.get("openalex_match_status", "")
                if progress is not None:
                    progress.update(1)
    finally:
        if progress is not None:
            progress.close()

    return authors_df


def main() -> None:
    args = parse_args()
    authors_csv = args.authors_csv or (args.input_dir / "authors.csv")
    edges_csv = args.edges_csv or (args.input_dir / "edges.csv")
    backfilled_authors_output = args.backfilled_authors_output or (
        args.input_dir / "authors_orcid_backfilled.csv"
    )
    orcid_authors_output = args.orcid_authors_output or (
        args.input_dir / "authors_orcid_subgraph.csv"
    )
    orcid_edges_output = args.orcid_edges_output or (
        args.input_dir / "edges_orcid_subgraph.csv"
    )
    community_output = args.community_output or (
        args.input_dir / "community_assignments_orcid_subgraph.csv"
    )
    local_orcid_cache = args.local_orcid_cache or (
        args.input_dir / "local_pid_orcid_cache.csv"
    )
    ror_cache = args.ror_cache or (
        args.input_dir / "ror_affiliation_cache.csv"
    )

    authors_df = pd.read_csv(authors_csv)
    edges_df = pd.read_csv(edges_csv)

    authors_df["dblp_pid"] = authors_df["dblp_pid"].fillna("").astype(str).str.strip()
    authors_df["orcid"] = authors_df["orcid"].fillna("").astype(str).str.strip()

    all_with_pid = authors_df["dblp_pid"] != ""
    target_pids = set(authors_df.loc[all_with_pid, "dblp_pid"].tolist())
    pid_to_orcid = load_local_orcid_cache(local_orcid_cache)
    cached_pids = {pid for pid, orcid in pid_to_orcid.items() if orcid}
    if pid_to_orcid:
        print(
            f"Loaded local pid->ORCID cache from {local_orcid_cache} "
            f"({len(pid_to_orcid)} pids, {len(cached_pids)} with ORCID)"
        )
    else:
        print(f"Scanning local DBLP dump for {len(target_pids)} author pids: {args.dblp_xml_gz}")
        pid_to_orcid = lookup_orcids_from_local_dump(args.dblp_xml_gz, target_pids)
        save_local_orcid_cache(local_orcid_cache, pid_to_orcid)
        print(f"Saved local pid->ORCID cache to {local_orcid_cache}")

    local_hits = sum(1 for pid in target_pids if pid_to_orcid.get(pid, ""))
    local_scanned = len(target_pids)
    local_misses = local_scanned - local_hits
    local_hit_rate = (local_hits / local_scanned * 100.0) if local_scanned else 0.0
    print(
        "[local-dblp] "
        f"target_pids={local_scanned} | "
        f"hits={local_hits} | "
        f"misses={local_misses} | "
        f"hit_rate={local_hit_rate:.2f}%"
    )

    local_hit_pids = {pid for pid, orcid in pid_to_orcid.items() if orcid}
    hit_mask = authors_df["dblp_pid"].isin(local_hit_pids)
    authors_df.loc[hit_mask, "orcid"] = authors_df.loc[hit_mask, "dblp_pid"].map(pid_to_orcid)

    backfilled_authors_output.parent.mkdir(parents=True, exist_ok=True)
    authors_df.to_csv(backfilled_authors_output, index=False, encoding="utf-8")

    orcid_authors_df = authors_df.loc[authors_df["orcid"] != ""].copy()
    orcid_authors_df = enrich_affiliations(
        orcid_authors_df,
        sleep_seconds=args.sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        max_workers=args.max_workers,
    )
    orcid_authors_df = enrich_ror_countries(
        orcid_authors_df,
        cache_path=ror_cache,
        sleep_seconds=args.sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
    )
    affiliation_mask = orcid_authors_df["affiliation"].fillna("").astype(str).str.strip() != ""
    affiliation_authors_df = orcid_authors_df.loc[affiliation_mask].copy()
    affiliation_author_ids = set(affiliation_authors_df["author_id"].tolist())
    orcid_edges_df = edges_df.loc[
        (edges_df["weight"] >= args.min_edge_weight)
        & edges_df["source_author_id"].isin(affiliation_author_ids)
        & edges_df["target_author_id"].isin(affiliation_author_ids)
    ].copy()

    orcid_graph = build_graph_from_frames(affiliation_authors_df, orcid_edges_df)
    connected_author_ids = set(orcid_graph.nodes()) - set(nx.isolates(orcid_graph))
    affiliation_authors_df = affiliation_authors_df.loc[
        affiliation_authors_df["author_id"].isin(connected_author_ids)
    ].copy()
    orcid_edges_df = orcid_edges_df.loc[
        orcid_edges_df["source_author_id"].isin(connected_author_ids)
        & orcid_edges_df["target_author_id"].isin(connected_author_ids)
    ].copy()
    orcid_graph = build_graph_from_frames(affiliation_authors_df, orcid_edges_df)
    weighted_degree = dict(orcid_graph.degree(weight="weight"))
    community_membership = detect_communities(orcid_graph)
    bridge_scores = compute_bridge_scores(orcid_graph, community_membership)

    affiliation_authors_df["weighted_degree"] = (
        affiliation_authors_df["author_id"].map(weighted_degree).fillna(0).astype(int)
    )
    orcid_authors_output.parent.mkdir(parents=True, exist_ok=True)
    affiliation_authors_df.to_csv(orcid_authors_output, index=False, encoding="utf-8")

    orcid_edges_output.parent.mkdir(parents=True, exist_ok=True)
    orcid_edges_df.to_csv(orcid_edges_output, index=False, encoding="utf-8")

    community_rows = []
    for row in affiliation_authors_df.itertuples(index=False):
        community_rows.append(
            {
                "author_id": row.author_id,
                "name": row.name,
                "dblp_pid": row.dblp_pid,
                "orcid": row.orcid,
                "paper_count": int(row.paper_count),
                "weighted_degree": int(weighted_degree.get(row.author_id, 0)),
                "community_id": int(community_membership[row.author_id]),
                "community_label": f"Community {int(community_membership[row.author_id]) + 1}",
                "bridge_score": float(bridge_scores.get(row.author_id, 0.0)),
            }
        )

    community_df = pd.DataFrame(community_rows).sort_values(
        by=["community_id", "weighted_degree", "paper_count", "name"],
        ascending=[True, False, False, True],
    )
    community_output.parent.mkdir(parents=True, exist_ok=True)
    community_df.to_csv(community_output, index=False, encoding="utf-8")

    authors_without_orcid = int((authors_df["orcid"] == "").sum())
    print(f"Saved all-author ORCID backfill to {backfilled_authors_output}")
    print(f"Saved ORCID-only author subgraph nodes to {orcid_authors_output}")
    print(f"Saved ORCID-only author subgraph edges to {orcid_edges_output}")
    print(f"Saved ORCID-subgraph community assignments to {community_output}")
    print(
        "[summary] "
        f"all_authors={len(authors_df)} | "
        f"authors_with_orcid={len(orcid_authors_df)} | "
        f"authors_with_affiliation={len(affiliation_authors_df)} | "
        f"authors_without_orcid={authors_without_orcid} | "
        f"orcid_subgraph_edges={len(orcid_edges_df)}"
    )


if __name__ == "__main__":
    main()
