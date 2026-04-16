#!/usr/bin/env python3
"""Enrich top authors with employer and nationality-like metadata.

Primary sources:
- OpenAlex Authors API for current / last known institution
- Wikidata API for country of citizenship

The script reads the exported coauthor dataset, selects the top authors by a
chosen metric, queries external APIs, and writes a CSV for manual review.
"""

from __future__ import annotations

import argparse
import csv
import json
import socket
import time
import urllib.parse
import urllib.request
from pathlib import Path
from urllib.error import HTTPError, URLError


USER_AGENT = "research-community-analysis/1.0 (author profile enrichment)"
OPENALEX_BASE = "https://api.openalex.org"
WIKIDATA_SEARCH_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{entity_id}.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich top authors with OpenAlex employer and Wikidata citizenship."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025"),
        help="Directory containing authors.csv and edges.csv.",
    )
    parser.add_argument(
        "--metric",
        choices=["paper_count", "weighted_degree"],
        default="paper_count",
        help="Ranking metric used to choose top authors.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of top authors to enrich.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.25,
        help="Delay between external API requests.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=45.0,
        help="HTTP timeout for each request.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=4,
        help="Maximum retries for transient network failures.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output CSV path. Defaults to <input-dir>/top_authors_profiles.csv.",
    )
    return parser.parse_args()


def fetch_json(
    url: str,
    params: dict[str, str] | None = None,
    timeout_seconds: float = 45.0,
    max_retries: int = 4,
) -> dict:
    if params:
        query = urllib.parse.urlencode(params)
        url = f"{url}?{query}"

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code in {429, 500, 502, 503, 504} and attempt < max_retries:
                time.sleep(min(8.0, 0.8 * (2 ** (attempt - 1))))
                last_error = exc
                continue
            raise
        except (URLError, TimeoutError, socket.timeout) as exc:
            if attempt < max_retries:
                time.sleep(min(8.0, 0.8 * (2 ** (attempt - 1))))
                last_error = exc
                continue
            last_error = exc
            break

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Failed to fetch JSON from {url}")


def normalize_name(name: str) -> str:
    return " ".join(name.casefold().replace("-", " ").split())


def clean_author_name(name: str) -> str:
    parts = name.rsplit(" ", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0].strip()
    return name.strip()


def normalize_orcid(orcid: str) -> str:
    value = orcid.strip()
    value = value.removeprefix("https://orcid.org/")
    value = value.removeprefix("http://orcid.org/")
    return value


def load_top_authors(input_dir: Path, metric: str, top_k: int) -> list[dict[str, str]]:
    authors_path = input_dir / "authors.csv"
    edges_path = input_dir / "edges.csv"

    if not authors_path.exists():
        raise FileNotFoundError(f"Missing file: {authors_path}")
    if metric == "weighted_degree" and not edges_path.exists():
        raise FileNotFoundError(f"Missing file: {edges_path}")

    with authors_path.open("r", encoding="utf-8", newline="") as handle:
        author_rows = list(csv.DictReader(handle))

    if metric == "paper_count":
        ranked = sorted(
            author_rows,
            key=lambda row: (-int(row["paper_count"]), row["name"]),
        )
        return ranked[:top_k]

    weighted_degree: dict[str, int] = {}
    with edges_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            weight = int(row["weight"])
            weighted_degree[row["source_author_id"]] = weighted_degree.get(row["source_author_id"], 0) + weight
            weighted_degree[row["target_author_id"]] = weighted_degree.get(row["target_author_id"], 0) + weight

    for row in author_rows:
        row["weighted_degree"] = str(weighted_degree.get(row["author_id"], 0))

    ranked = sorted(
        author_rows,
        key=lambda row: (-int(row["weighted_degree"]), -int(row["paper_count"]), row["name"]),
    )
    return ranked[:top_k]


def choose_best_openalex_match(results: list[dict], target_name: str) -> dict | None:
    if not results:
        return None

    target = normalize_name(target_name)

    def score(result: dict) -> tuple[int, int, int, str]:
        display_name = result.get("display_name", "")
        normalized = normalize_name(display_name)
        exact = int(normalized == target)
        prefix = int(normalized.startswith(target) or target.startswith(normalized))
        cited_by_count = int(result.get("cited_by_count") or 0)
        works_count = int(result.get("works_count") or 0)
        return (exact, prefix, cited_by_count + works_count, display_name)

    return max(results, key=score)


def lookup_openalex_author(
    orcid: str,
    name: str,
    timeout_seconds: float,
    max_retries: int,
) -> dict[str, str]:
    normalized_orcid = normalize_orcid(orcid)
    result = None
    match_status = "not_found"
    if normalized_orcid:
        try:
            result = fetch_json(
                f"{OPENALEX_BASE}/authors/orcid:{normalized_orcid}",
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
            )
            match_status = "matched_by_orcid"
        except HTTPError as exc:
            if exc.code != 404:
                raise

    if result is None:
        payload = fetch_json(
            f"{OPENALEX_BASE}/authors",
            {"search": name, "per-page": "5"},
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        result = choose_best_openalex_match(payload.get("results", []), name)
        match_status = "matched_by_name" if result else "not_found"

    if not result:
        return {
            "openalex_id": "",
            "openalex_display_name": "",
            "employer": "",
            "all_employers": "",
            "employer_country_code": "",
            "employer_count": "0",
            "openalex_affiliations_count": "0",
            "employer_status": "not_found",
            "openalex_query_type": "orcid" if normalized_orcid else "name",
            "openalex_match_status": "not_found",
        }

    institutions = result.get("last_known_institutions") or []
    affiliations = result.get("affiliations") or []
    institution_names = [
        institution.get("display_name", "").strip()
        for institution in institutions
        if institution.get("display_name")
    ]
    institution_country_codes = [
        institution.get("country_code", "").strip()
        for institution in institutions
        if institution.get("country_code")
    ]

    fallback_name = ""
    fallback_country_code = ""
    if not institution_names and affiliations:
        affiliation_candidates: list[tuple[int, int, str, str]] = []
        for affiliation in affiliations:
            institution = affiliation.get("institution") or {}
            display_name = institution.get("display_name", "").strip()
            if not display_name:
                continue
            years = affiliation.get("years") or []
            years = [int(year) for year in years if str(year).isdigit()]
            most_recent_year = max(years) if years else -1
            frequency = len(years)
            affiliation_candidates.append(
                (
                    most_recent_year,
                    frequency,
                    display_name,
                    institution.get("country_code", "").strip(),
                )
            )

        if affiliation_candidates:
            affiliation_candidates.sort(
                key=lambda item: (item[0], item[1], item[2]),
                reverse=True,
            )
            fallback_name = affiliation_candidates[0][2]
            fallback_country_code = affiliation_candidates[0][3]

    employer_names = institution_names if institution_names else ([fallback_name] if fallback_name else [])
    employer_country_codes = (
        institution_country_codes
        if institution_country_codes
        else ([fallback_country_code] if fallback_country_code else [])
    )
    employer_source = "last_known_institutions_primary" if institution_names else (
        "affiliations_fallback" if fallback_name else "missing"
    )
    return {
        "openalex_id": str(result.get("id", "")),
        "openalex_display_name": result.get("display_name", ""),
        "employer": employer_names[0] if employer_names else "",
        "all_employers": "|".join(dict.fromkeys(employer_names)),
        "employer_country_code": employer_country_codes[0] if employer_country_codes else "",
        "employer_count": str(len(employer_names)),
        "openalex_affiliations_count": str(len(affiliations)),
        "employer_source": employer_source,
        "openalex_query_type": "orcid" if normalized_orcid else "name",
        "employer_status": (
            "found"
            if employer_names
            else "missing_last_known_institutions"
        ),
        "openalex_match_status": match_status,
    }


def choose_best_wikidata_match(search_results: list[dict], target_name: str) -> dict | None:
    if not search_results:
        return None

    target = normalize_name(target_name)

    def score(result: dict) -> tuple[int, int, int]:
        label = normalize_name(result.get("label", ""))
        description = normalize_name(result.get("description", ""))
        exact = int(label == target)
        has_researcher_hint = int(
            any(token in description for token in ["researcher", "scientist", "professor", "computer"])
        )
        return (exact, has_researcher_hint, -len(label))

    return max(search_results, key=score)


def extract_wikidata_value_labels(entity: dict, prop: str) -> list[str]:
    claims = entity.get("claims", {}).get(prop, [])
    labels: list[str] = []
    for claim in claims:
        mainsnak = claim.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        value = datavalue.get("value", {})
        if isinstance(value, dict):
            entity_id = value.get("id")
            if entity_id:
                labels.append(entity_id)
    return labels


def resolve_wikidata_entity_labels(entity_ids: list[str]) -> dict[str, str]:
    if not entity_ids:
        return {}
    payload = fetch_json(
        WIKIDATA_SEARCH_URL,
        {
            "action": "wbgetentities",
            "format": "json",
            "ids": "|".join(entity_ids),
            "props": "labels",
            "languages": "en",
        },
    )
    resolved: dict[str, str] = {}
    for entity_id, entity in payload.get("entities", {}).items():
        resolved[entity_id] = (
            entity.get("labels", {}).get("en", {}).get("value", "")
        )
    return resolved


def lookup_wikidata_citizenship(
    name: str,
    timeout_seconds: float,
    max_retries: int,
) -> dict[str, str]:
    search_payload = fetch_json(
        WIKIDATA_SEARCH_URL,
        {
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "type": "item",
            "limit": "5",
            "search": name,
        },
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
    result = choose_best_wikidata_match(search_payload.get("search", []), name)
    if not result:
        return {
            "wikidata_id": "",
            "wikidata_label": "",
            "country_of_citizenship": "",
            "wikidata_match_status": "not_found",
        }

    entity_id = result.get("id", "")
    entity_payload = fetch_json(
        WIKIDATA_ENTITY_URL.format(entity_id=entity_id),
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
    entity = entity_payload.get("entities", {}).get(entity_id, {})
    citizenship_ids = extract_wikidata_value_labels(entity, "P27")
    citizenship_labels = resolve_wikidata_entity_labels(citizenship_ids)

    return {
        "wikidata_id": entity_id,
        "wikidata_label": result.get("label", ""),
        "country_of_citizenship": "|".join(
            label for label in (citizenship_labels.get(item_id, "") for item_id in citizenship_ids) if label
        ),
        "wikidata_match_status": "matched",
    }


def enrich_author(
    row: dict[str, str],
    metric: str,
    sleep_seconds: float,
    timeout_seconds: float,
    max_retries: int,
) -> dict[str, str]:
    name = row["name"]
    query_name = clean_author_name(name)
    record = {
        "author_id": row["author_id"],
        "name": name,
        "query_name": query_name,
        "dblp_pid": row.get("dblp_pid", ""),
        "orcid": row.get("orcid", ""),
        "paper_count": row.get("paper_count", ""),
        "metric": metric,
        "metric_value": row.get(metric, row.get("paper_count", "")),
    }

    try:
        openalex_fields = lookup_openalex_author(
            row.get("orcid", ""),
            query_name,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
    except Exception as exc:
        openalex_fields = {
            "openalex_id": "",
            "openalex_display_name": "",
            "employer": "",
            "all_employers": "",
            "employer_country_code": "",
            "employer_count": "0",
            "openalex_affiliations_count": "0",
            "employer_source": "error",
            "openalex_query_type": "orcid" if row.get("orcid", "") else "name",
            "employer_status": f"error:{type(exc).__name__}",
            "openalex_match_status": f"error:{type(exc).__name__}",
        }
    time.sleep(sleep_seconds)

    try:
        wikidata_fields = lookup_wikidata_citizenship(
            query_name,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
    except Exception as exc:
        wikidata_fields = {
            "wikidata_id": "",
            "wikidata_label": "",
            "country_of_citizenship": "",
            "wikidata_match_status": f"error:{type(exc).__name__}",
        }
    time.sleep(sleep_seconds)

    record.update(openalex_fields)
    record.update(wikidata_fields)
    return record


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_author_summary(record: dict[str, str]) -> None:
    employer = record.get("employer") or "(none)"
    citizenship = record.get("country_of_citizenship") or "(none)"
    openalex_status = record.get("openalex_match_status") or "(unknown)"
    employer_status = record.get("employer_status") or "(unknown)"
    employer_source = record.get("employer_source") or "(unknown)"
    wikidata_status = record.get("wikidata_match_status") or "(unknown)"
    employer_count = record.get("employer_count") or "0"
    affiliations_count = record.get("openalex_affiliations_count") or "0"
    print(
        f"  employer={employer} | citizenship={citizenship} | "
        f"openalex={openalex_status} | query_type={record.get('openalex_query_type') or '(unknown)'} | employer_status={employer_status} | "
        f"employer_source={employer_source} | "
        f"employer_count={employer_count} | affiliations_count={affiliations_count} | "
        f"wikidata={wikidata_status}"
    )


def main() -> None:
    args = parse_args()
    output_path = args.output or (args.input_dir / "top_authors_profiles.csv")

    top_authors = load_top_authors(args.input_dir, metric=args.metric, top_k=args.top_k)
    rows = []
    for index, row in enumerate(top_authors, start=1):
        query_name = clean_author_name(row["name"])
        print(f"[{index}/{len(top_authors)}] enriching {row['name']} -> query={query_name}")
        rows.append(
            enrich_author(
                row,
                metric=args.metric,
                sleep_seconds=args.sleep_seconds,
                timeout_seconds=args.timeout_seconds,
                max_retries=args.max_retries,
            )
        )
        print_author_summary(rows[-1])

    write_csv(output_path, rows)
    print(f"Saved enriched author profiles to {output_path}")


if __name__ == "__main__":
    main()
