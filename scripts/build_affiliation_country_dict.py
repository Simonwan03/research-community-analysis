#!/usr/bin/env python3
"""Build a deduplicated affiliation -> country mapping for ORCID authors."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd

from enrich_top_authors_profiles import fetch_json
from manual_affiliation_country_overrides import MANUAL_AFFILIATION_COUNTRY_OVERRIDES

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect all affiliations from authors with ORCID, deduplicate them, "
            "query ROR for country information, and save the result as a loadable dict."
        )
    )
    parser.add_argument(
        "--authors-csv",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025/authors_orcid_subgraph.csv"),
        help="Author CSV containing ORCID authors and affiliation fields.",
    )
    parser.add_argument(
        "--affiliations-csv",
        type=Path,
        default=None,
        help="Optional CSV containing a single affiliation_label column to query directly.",
    )
    parser.add_argument(
        "--unique-affiliations-output",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025/unique_affiliations.csv"),
        help="CSV path for the deduplicated affiliation list.",
    )
    parser.add_argument(
        "--ror-csv-output",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025/affiliation_country_lookup.csv"),
        help="CSV path for affiliation -> country lookup results.",
    )
    parser.add_argument(
        "--dict-json-output",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025/affiliation_country_dict.json"),
        help="JSON path for a dict-style affiliation -> country mapping.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.25,
        help="Delay between ROR requests. Default: 0.25",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=60.0,
        help="HTTP timeout for each ROR request. Default: 60.0",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum retries for transient ROR request failures. Default: 5",
    )
    return parser.parse_args()


def split_affiliations(text: str) -> list[str]:
    return [part.strip() for part in str(text).split("|") if part.strip()]


def collect_unique_affiliations(authors_df: pd.DataFrame) -> list[str]:
    unique_affiliations: set[str] = set()

    for row in authors_df.itertuples(index=False):
        primary = str(getattr(row, "affiliation", "")).strip()
        if primary:
            unique_affiliations.add(primary)

        all_affiliations = str(getattr(row, "all_affiliations", "")).strip()
        unique_affiliations.update(split_affiliations(all_affiliations))

    return sorted(unique_affiliations)


def lookup_ror_affiliation(
    affiliation_label: str,
    timeout_seconds: float,
    max_retries: int,
) -> dict[str, str]:
    if affiliation_label in MANUAL_AFFILIATION_COUNTRY_OVERRIDES:
        manual = MANUAL_AFFILIATION_COUNTRY_OVERRIDES[affiliation_label]
        return {
            "affiliation_label": affiliation_label,
            "ror_id": manual.get("ror_id", ""),
            "ror_name": manual.get("ror_name", ""),
            "country_code": manual.get("country_code", ""),
            "country_name": manual.get("country_name", ""),
            "match_status": manual.get("match_status", "manual"),
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
            "affiliation_label": affiliation_label,
            "ror_id": "",
            "ror_name": "",
            "country_code": "",
            "country_name": "",
            "match_status": "not_found",
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
        "affiliation_label": affiliation_label,
        "ror_id": str(organization.get("id", "")).strip(),
        "ror_name": str(name).strip(),
        "country_code": str(geonames.get("country_code", "")).strip(),
        "country_name": str(geonames.get("country_name", "")).strip(),
        "match_status": "matched",
    }


def main() -> None:
    args = parse_args()
    if args.affiliations_csv is not None:
        affiliations_df = pd.read_csv(args.affiliations_csv, low_memory=False)
        if "affiliation_label" not in affiliations_df.columns:
            raise SystemExit("affiliations CSV must contain an 'affiliation_label' column.")
        unique_affiliations = sorted(
            {
                str(value).strip()
                for value in affiliations_df["affiliation_label"].fillna("").astype(str)
                if str(value).strip()
            }
        )
        orcid_authors_count = 0
    else:
        authors_df = pd.read_csv(args.authors_csv, low_memory=False)

        if "orcid" not in authors_df.columns:
            raise SystemExit("authors CSV must contain an 'orcid' column.")
        if "affiliation" not in authors_df.columns and "all_affiliations" not in authors_df.columns:
            raise SystemExit(
                "authors CSV must contain at least one of 'affiliation' or 'all_affiliations'."
            )

        authors_df["orcid"] = authors_df["orcid"].fillna("").astype(str).str.strip()
        orcid_authors_df = authors_df.loc[authors_df["orcid"] != ""].copy()
        unique_affiliations = collect_unique_affiliations(orcid_authors_df)
        orcid_authors_count = len(orcid_authors_df)

    args.unique_affiliations_output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"affiliation_label": unique_affiliations}).to_csv(
        args.unique_affiliations_output,
        index=False,
        encoding="utf-8",
    )

    results: list[dict[str, str]] = []
    progress = (
        tqdm(total=len(unique_affiliations), desc="ROR affiliation->country", unit="affiliation")
        if tqdm is not None
        else None
    )
    try:
        for affiliation_label in unique_affiliations:
            try:
                result = lookup_ror_affiliation(
                    affiliation_label,
                    timeout_seconds=args.timeout_seconds,
                    max_retries=args.max_retries,
                )
            except Exception as exc:
                result = {
                    "affiliation_label": affiliation_label,
                    "ror_id": "",
                    "ror_name": "",
                    "country_code": "",
                    "country_name": "",
                    "match_status": f"error:{type(exc).__name__}",
                }
            results.append(result)
            if progress is not None:
                progress.update(1)
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
    finally:
        if progress is not None:
            progress.close()

    lookup_df = pd.DataFrame(results).sort_values(by=["affiliation_label"])
    args.ror_csv_output.parent.mkdir(parents=True, exist_ok=True)
    lookup_df.to_csv(args.ror_csv_output, index=False, encoding="utf-8")

    mapping = {
        row["affiliation_label"]: {
            "ror_id": row["ror_id"],
            "ror_name": row["ror_name"],
            "country_code": row["country_code"],
            "country_name": row["country_name"],
            "match_status": row["match_status"],
        }
        for row in results
    }
    args.dict_json_output.parent.mkdir(parents=True, exist_ok=True)
    args.dict_json_output.write_text(
        json.dumps(mapping, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    matched = sum(1 for row in results if row["match_status"] == "matched")
    not_found = sum(1 for row in results if row["match_status"] == "not_found")
    errors = sum(1 for row in results if row["match_status"].startswith("error:"))
    print(f"Saved unique affiliations to {args.unique_affiliations_output}")
    print(f"Saved ROR lookup CSV to {args.ror_csv_output}")
    print(f"Saved affiliation country dict JSON to {args.dict_json_output}")
    print(
        "[summary] "
        f"orcid_authors={orcid_authors_count} | "
        f"unique_affiliations={len(unique_affiliations)} | "
        f"matched={matched} | "
        f"not_found={not_found} | "
        f"errors={errors}"
    )


if __name__ == "__main__":
    main()
