#!/usr/bin/env python3
"""Generate a fullgraph author CSV with affiliation metadata for all backfilled authors."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import time

import pandas as pd

from enrich_top_authors_profiles import lookup_openalex_author
from generate_orcid_subgraph_communities import load_ror_cache, save_ror_cache, split_affiliations
from manual_affiliation_country_overrides import MANUAL_AFFILIATION_COUNTRY_OVERRIDES

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


AFFILIATION_COLUMNS = [
    "openalex_id",
    "openalex_display_name",
    "affiliation",
    "all_affiliations",
    "affiliation_country_code",
    "affiliation_source",
    "openalex_match_status",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate authors_orcid_fullgraph.csv from authors_orcid_backfilled.csv "
            "with affiliation metadata for all authors."
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
        help="Author CSV path. Defaults to <input-dir>/authors_orcid_backfilled.csv.",
    )
    parser.add_argument(
        "--edges-csv",
        type=Path,
        default=None,
        help="Edge CSV path. Defaults to <input-dir>/edges.csv.",
    )
    parser.add_argument(
        "--seed-affiliation-csv",
        type=Path,
        default=None,
        help=(
            "Existing enriched author CSV used as a warm-start cache. "
            "Defaults to <input-dir>/authors_orcid_subgraph.csv."
        ),
    )
    parser.add_argument(
        "--country-lookup-csv",
        type=Path,
        default=None,
        help="Affiliation-country lookup CSV. Defaults to <input-dir>/affiliation_country_lookup.csv.",
    )
    parser.add_argument(
        "--author-affiliation-cache",
        type=Path,
        default=None,
        help="Persistent cache CSV for author affiliation lookups. Defaults to <input-dir>/author_affiliation_cache_fullgraph.csv.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path. Defaults to <input-dir>/authors_orcid_fullgraph.csv.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.25,
        help="Delay after each OpenAlex request. Default: 0.25",
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
        "--save-every",
        type=int,
        default=100,
        help="Persist cache after every N new lookup results. Default: 100",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Only process the first N ORCID authors missing from cache. Useful for testing.",
    )
    return parser.parse_args()


def load_country_lookup(country_lookup_csv: Path) -> dict[str, dict[str, str]]:
    if not country_lookup_csv.exists():
        return {}
    lookup_df = pd.read_csv(country_lookup_csv, low_memory=False).fillna("")
    return {
        str(row.affiliation_label).strip(): {
            "country_code": str(row.country_code).strip(),
            "country_name": str(row.country_name).strip(),
        }
        for row in lookup_df.itertuples(index=False)
        if str(row.affiliation_label).strip()
    }


def lookup_ror_affiliation(
    affiliation_label: str,
    country_lookup: dict[str, dict[str, str]],
) -> dict[str, str]:
    if affiliation_label in MANUAL_AFFILIATION_COUNTRY_OVERRIDES:
        manual = MANUAL_AFFILIATION_COUNTRY_OVERRIDES[affiliation_label]
        return {
            "country_code": manual.get("country_code", ""),
            "country_name": manual.get("country_name", ""),
        }
    return country_lookup.get(affiliation_label, {})


def load_seed_rows(seed_csv: Path | None) -> dict[str, dict[str, str]]:
    if seed_csv is None or not seed_csv.exists():
        return {}
    seed_df = pd.read_csv(seed_csv, low_memory=False).fillna("")
    if "author_id" not in seed_df.columns:
        return {}
    rows: dict[str, dict[str, str]] = {}
    for row in seed_df.to_dict(orient="records"):
        author_id = str(row.get("author_id", "")).strip()
        if not author_id:
            continue
        rows[author_id] = {
            column: str(row.get(column, "")).strip()
            for column in AFFILIATION_COLUMNS
        }
    return rows


def load_affiliation_cache(cache_csv: Path, seed_rows: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    rows = dict(seed_rows)
    if cache_csv.exists():
        cache_df = pd.read_csv(cache_csv, low_memory=False).fillna("")
        for row in cache_df.to_dict(orient="records"):
            author_id = str(row.get("author_id", "")).strip()
            if not author_id:
                continue
            rows[author_id] = {
                column: str(row.get(column, "")).strip()
                for column in AFFILIATION_COLUMNS
            }
    return rows


def save_affiliation_cache(cache_csv: Path, cache: dict[str, dict[str, str]]) -> None:
    rows = []
    for author_id in sorted(cache):
        row = {"author_id": author_id}
        row.update(cache[author_id])
        rows.append(row)
    cache_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(cache_csv, index=False, encoding="utf-8")


def compute_weighted_degree(edges_csv: Path) -> dict[str, int]:
    edges_df = pd.read_csv(edges_csv, low_memory=False)
    weighted_degree: dict[str, int] = {}
    for row in edges_df.itertuples(index=False):
        weight = int(getattr(row, "weight", 0))
        source = str(getattr(row, "source_author_id", "")).strip()
        target = str(getattr(row, "target_author_id", "")).strip()
        if source:
            weighted_degree[source] = weighted_degree.get(source, 0) + weight
        if target:
            weighted_degree[target] = weighted_degree.get(target, 0) + weight
    return weighted_degree


def fill_country_code(
    fields: dict[str, str],
    country_lookup: dict[str, dict[str, str]],
) -> dict[str, str]:
    affiliation = str(fields.get("affiliation", "")).strip()
    if fields.get("affiliation_country_code", "") or not affiliation:
        return fields
    lookup = lookup_ror_affiliation(affiliation, country_lookup)
    if lookup.get("country_code"):
        fields["affiliation_country_code"] = lookup["country_code"]
    return fields


def main() -> None:
    args = parse_args()
    authors_csv = args.authors_csv or (args.input_dir / "authors_orcid_backfilled.csv")
    edges_csv = args.edges_csv or (args.input_dir / "edges.csv")
    seed_affiliation_csv = args.seed_affiliation_csv or (args.input_dir / "authors_orcid_subgraph.csv")
    country_lookup_csv = args.country_lookup_csv or (args.input_dir / "affiliation_country_lookup.csv")
    cache_csv = args.author_affiliation_cache or (args.input_dir / "author_affiliation_cache_fullgraph.csv")
    output_csv = args.output or (args.input_dir / "authors_orcid_fullgraph.csv")

    authors_df = pd.read_csv(authors_csv, low_memory=False).fillna("")
    authors_df["orcid"] = authors_df["orcid"].astype(str).str.strip()
    authors_df["author_id"] = authors_df["author_id"].astype(str).str.strip()

    weighted_degree = compute_weighted_degree(edges_csv)
    country_lookup = load_country_lookup(country_lookup_csv)
    seed_rows = load_seed_rows(seed_affiliation_csv)
    affiliation_cache = load_affiliation_cache(cache_csv, seed_rows)

    for column in AFFILIATION_COLUMNS:
        authors_df[column] = ""

    orcid_mask = authors_df["orcid"] != ""
    missing_mask = ~authors_df["author_id"].isin(affiliation_cache.keys())
    pending_df = authors_df.loc[orcid_mask & missing_mask, ["author_id", "name", "orcid"]].copy()
    pending_df = pending_df.sort_values(by=["author_id", "name"], ascending=[True, True])
    if args.limit is not None:
        pending_df = pending_df.head(args.limit)

    print(
        "[prep] "
        f"all_authors={len(authors_df)} | "
        f"authors_with_orcid={int(orcid_mask.sum())} | "
        f"seed_cached_authors={len(seed_rows)} | "
        f"cache_authors={len(affiliation_cache)} | "
        f"pending_openalex_queries={len(pending_df)}"
    )

    def fetch_one(author_id: str, name: str, orcid: str) -> tuple[str, dict[str, str]]:
        import time as _time

        try:
            raw = lookup_openalex_author(
                orcid,
                name,
                timeout_seconds=args.timeout_seconds,
                max_retries=args.max_retries,
            )
            fields = {
                "openalex_id": str(raw.get("openalex_id", "")).strip(),
                "openalex_display_name": str(raw.get("openalex_display_name", "")).strip(),
                "affiliation": str(raw.get("employer", "")).strip(),
                "all_affiliations": str(raw.get("all_employers", "")).strip(),
                "affiliation_country_code": str(raw.get("employer_country_code", "")).strip(),
                "affiliation_source": str(raw.get("employer_source", "")).strip(),
                "openalex_match_status": str(raw.get("openalex_match_status", "")).strip(),
            }
        except Exception as exc:
            fields = {
                "openalex_id": "",
                "openalex_display_name": "",
                "affiliation": "",
                "all_affiliations": "",
                "affiliation_country_code": "",
                "affiliation_source": "error",
                "openalex_match_status": f"error:{type(exc).__name__}",
            }
        fields = fill_country_code(fields, country_lookup)
        if args.sleep_seconds > 0:
            _time.sleep(args.sleep_seconds)
        return author_id, fields

    if len(pending_df) > 0:
        progress = tqdm(total=len(pending_df), desc="OpenAlex fullgraph affiliation", unit="author") if tqdm is not None else None
        new_since_save = 0
        try:
            with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
                futures = [
                    executor.submit(fetch_one, row.author_id, str(row.name), str(row.orcid))
                    for row in pending_df.itertuples(index=False)
                ]
                for future in as_completed(futures):
                    author_id, fields = future.result()
                    affiliation_cache[author_id] = fields
                    new_since_save += 1
                    if progress is not None:
                        progress.update(1)
                    if new_since_save >= max(1, args.save_every):
                        save_affiliation_cache(cache_csv, affiliation_cache)
                        new_since_save = 0
        finally:
            if progress is not None:
                progress.close()
        save_affiliation_cache(cache_csv, affiliation_cache)
    else:
        save_affiliation_cache(cache_csv, affiliation_cache)

    for author_id, fields in affiliation_cache.items():
        mask = authors_df["author_id"] == author_id
        for column in AFFILIATION_COLUMNS:
            authors_df.loc[mask, column] = fields.get(column, "")

    authors_df["weighted_degree"] = (
        authors_df["author_id"].map(weighted_degree).fillna(0).astype(int)
    )

    output_columns = [
        "author_id",
        "name",
        "dblp_pid",
        "orcid",
        "paper_count",
        "paper_ids",
        "venues",
        "years",
        "openalex_id",
        "openalex_display_name",
        "affiliation",
        "all_affiliations",
        "affiliation_country_code",
        "affiliation_source",
        "openalex_match_status",
        "weighted_degree",
    ]
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    authors_df[output_columns].to_csv(output_csv, index=False, encoding="utf-8")

    with_affiliation = int((authors_df["affiliation"].astype(str).str.strip() != "").sum())
    with_country_code = int((authors_df["affiliation_country_code"].astype(str).str.strip() != "").sum())
    print(f"Saved fullgraph author CSV to {output_csv}")
    print(
        "[summary] "
        f"all_authors={len(authors_df)} | "
        f"authors_with_orcid={int(orcid_mask.sum())} | "
        f"authors_with_affiliation={with_affiliation} | "
        f"authors_with_country_code={with_country_code}"
    )


if __name__ == "__main__":
    main()
