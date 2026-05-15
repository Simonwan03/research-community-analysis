#!/usr/bin/env python3
"""Fetch DBLP conference papers/authors and build a coauthor graph.

This script pulls papers from the DBLP proceedings XML for a small set of AI
conferences, filters by year, and exports:

- papers.csv
- authors.csv
- edges.csv
- graph.graphml
- summary.json

Default venue slice:
    AAAI, IJCAI, ICML, NeurIPS, ICLR

Default year slice:
    2015 -> current year

Notes:
- DBLP exposes per-year proceedings records at `/rec/conf/<venue>/<year>.xml`.
- Each proceedings record contains a `url` like `db/conf/aaai/aaai2025.html`.
- Replacing `.html` with `.xml` yields the per-year table-of-contents XML that
  contains the paper-level `<inproceedings>` entries.
- DBLP rate limits aggressively. The script uses a polite user agent, sleeps
  between requests, and retries on 429/5xx responses.
"""

from __future__ import annotations

import argparse
import csv
import html
import itertools
import json
import re
import subprocess
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

DBLP_BASE_URL = "https://dblp.org/"
DEFAULT_VENUES = {
    "aaai": "AAAI",
    "ijcai": "IJCAI",
    "icml": "ICML",
    "nips": "NeurIPS",
    "iclr": "ICLR",
}
USER_AGENT = (
    "research-community-analysis/1.0 "
    "(polite DBLP fetcher for academic analysis)"
)


@dataclass
class PaperRecord:
    paper_id: str
    dblp_key: str
    title: str
    year: int
    venue_key: str
    venue_name: str
    booktitle: str
    ee: str
    pages: str
    crossref: str
    dblp_url: str
    toc_url: str
    author_ids: list[str]
    author_names: list[str]
    paper_xml_author_orcids: list[str]
    author_orcids: list[str]


def parse_args() -> argparse.Namespace:
    current_year = datetime.now().year
    parser = argparse.ArgumentParser(
        description="Fetch DBLP conference papers/authors and build a coauthor graph."
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2025,
        help="Inclusive start year. Default: 2015",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2025,
        help=f"Inclusive end year. Default: {current_year}",
    )
    parser.add_argument(
        "--venues",
        nargs="+",
        default=list(DEFAULT_VENUES.keys()),
        help=(
            "DBLP venue keys to fetch. "
            f"Default: {' '.join(DEFAULT_VENUES.keys())}"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory. Defaults to data/dblp_ai_authors_<start>_<end>",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=10.0,
        help="Delay between successful DBLP requests. Default: 10.0",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds. Default: 60",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=6,
        help="Max retries for 429/5xx/network errors. Default: 6",
    )
    parser.add_argument(
        "--orcid-sleep-seconds",
        type=float,
        default=10.0,
        help="Delay between DBLP pid->ORCID lookups. Default: 10.0",
    )
    return parser.parse_args()


class DblpClient:
    def __init__(self, sleep_seconds: float, timeout_seconds: float, max_retries: int):
        self.sleep_seconds = sleep_seconds
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self._last_request_at = 0.0

    def get_text(self, url: str) -> str:
        for attempt in range(1, self.max_retries + 1):
            self._sleep_if_needed()
            try:
                status_code, text = self._curl_get(url)
            except RuntimeError as exc:
                if attempt == self.max_retries:
                    raise
                self._backoff(attempt, reason=str(exc))
                continue
            self._last_request_at = time.time()

            if status_code == 404:
                raise FileNotFoundError(url)

            if status_code == 200 and "Too Many Requests" not in text:
                return text

            if status_code in {200, 429, 500, 502, 503, 504}:
                reason = "body says 429" if "Too Many Requests" in text else f"HTTP {status_code}"
                self._backoff(attempt, reason=reason)
                continue

            raise RuntimeError(
                f"Unexpected status for {url}: {status_code}\n{text[:200]}"
            )

        raise RuntimeError(f"Exhausted retries for {url}")

    def _curl_get(self, url: str) -> tuple[int, str]:
        command = [
            "curl",
            "-sS",
            "-L",
            "--compressed",
            "-A",
            USER_AGENT,
            "--connect-timeout",
            "20",
            "--max-time",
            str(int(self.timeout_seconds)),
            "-w",
            "\n__CURL_STATUS__:%{http_code}",
            url,
        ]
        try:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
            )
        except OSError as exc:
            raise RuntimeError(f"Failed to execute curl for {url}: {exc}") from exc

        output = result.stdout
        marker = "\n__CURL_STATUS__:"
        if marker not in output:
            stderr = result.stderr.strip()
            raise RuntimeError(f"curl did not return status marker for {url}: {stderr}")

        body, status_text = output.rsplit(marker, 1)
        try:
            status_code = int(status_text.strip())
        except ValueError as exc:
            raise RuntimeError(
                f"Unable to parse curl status for {url}: {status_text!r}"
            ) from exc

        if result.returncode != 0 and status_code == 0:
            stderr = result.stderr.strip()
            raise RuntimeError(f"curl failed for {url}: {stderr}")

        return status_code, body

    def _sleep_if_needed(self) -> None:
        elapsed = time.time() - self._last_request_at
        remaining = self.sleep_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _backoff(self, attempt: int, retry_after: str | None = None, reason: str = "") -> None:
        if retry_after:
            try:
                delay = max(float(retry_after), self.sleep_seconds)
            except ValueError:
                delay = self.sleep_seconds * (2 ** (attempt - 1))
        else:
            delay = self.sleep_seconds * (2 ** (attempt - 1))
        print(f"[retry] {reason or 'retrying'} -> sleeping {delay:.1f}s")
        time.sleep(delay)


def normalized_text(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


def safe_slug(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip()).strip("-").lower()
    return cleaned or "unknown"


def graphml_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def normalize_orcid(orcid: str | None) -> str:
    value = normalized_text(orcid)
    value = value.removeprefix("https://orcid.org/")
    value = value.removeprefix("http://orcid.org/")
    return value


def looks_like_orcid(value: str) -> bool:
    parts = value.split("-")
    if len(parts) != 4:
        return False
    return (
        all(part.isdigit() and len(part) == 4 for part in parts[:3])
        and len(parts[3]) == 4
        and all(char.isdigit() or char == "X" for char in parts[3])
    )


def extract_orcid_from_person_record(xml_text: str) -> str:
    root = ET.fromstring(xml_text)
    for element in root.iter():
        if element.tag not in {"url", "ee"}:
            continue
        candidate = normalize_orcid(element.text)
        if looks_like_orcid(candidate):
            return candidate
    return ""


def lookup_pid_orcid(client: DblpClient, dblp_pid: str) -> str:
    if not dblp_pid:
        return ""
    person_xml_url = urljoin(DBLP_BASE_URL, f"pid/{dblp_pid}.xml")
    xml_text = client.get_text(person_xml_url)
    return extract_orcid_from_person_record(xml_text)


def author_identity(author_elem: ET.Element) -> tuple[str, str, str, str]:
    name = normalized_text(author_elem.text)
    pid = author_elem.attrib.get("pid", "").strip()
    orcid = normalize_orcid(author_elem.attrib.get("orcid", ""))
    author_id = f"pid:{pid}" if pid else f"name:{name.casefold()}"
    return author_id, name, pid, orcid


def extract_toc_url(client: DblpClient, venue_key: str, year: int) -> str | None:
    proceedings_url = urljoin(DBLP_BASE_URL, f"rec/conf/{venue_key}/{year}.xml")
    try:
        xml_text = client.get_text(proceedings_url)
    except FileNotFoundError:
        return None

    root = ET.fromstring(xml_text)
    proceedings = root.find("./proceedings")
    if proceedings is None:
        return None

    toc_path = normalized_text(proceedings.findtext("url"))
    if not toc_path:
        return None
    toc_path = toc_path.replace(".html", ".xml")
    return urljoin(DBLP_BASE_URL, toc_path)


def iter_papers_from_toc(
    client: DblpClient,
    toc_url: str,
    venue_key: str,
    venue_name: str,
    year: int,
) -> Iterable[PaperRecord]:
    xml_text = client.get_text(toc_url)
    root = ET.fromstring(xml_text)

    for entry in root.iter("inproceedings"):
        paper_year = normalized_text(entry.findtext("year"))
        if not paper_year.isdigit() or int(paper_year) != year:
            continue

        authors: list[str] = []
        author_names: list[str] = []
        paper_xml_author_orcids: list[str] = []
        for author_elem in entry.findall("author"):
            author_id, author_name, _, author_orcid = author_identity(author_elem)
            authors.append(author_id)
            author_names.append(author_name)
            paper_xml_author_orcids.append(author_orcid)

        if not authors:
            continue

        dblp_key = entry.attrib.get("key", "").strip()
        if not dblp_key:
            continue

        paper_id = safe_slug(dblp_key.replace("/", "-"))
        yield PaperRecord(
            paper_id=paper_id,
            dblp_key=dblp_key,
            title=normalized_text(entry.findtext("title")),
            year=year,
            venue_key=venue_key,
            venue_name=venue_name,
            booktitle=normalized_text(entry.findtext("booktitle")) or venue_name,
            ee=normalized_text(entry.findtext("ee")),
            pages=normalized_text(entry.findtext("pages")),
            crossref=normalized_text(entry.findtext("crossref")),
            dblp_url=urljoin(DBLP_BASE_URL, normalized_text(entry.findtext("url"))),
            toc_url=toc_url,
            author_ids=authors,
            author_names=author_names,
            paper_xml_author_orcids=paper_xml_author_orcids,
            author_orcids=list(paper_xml_author_orcids),
        )


def resolve_author_orcids(
    client: DblpClient,
    authors: dict[str, dict],
    papers_to_update: list[PaperRecord],
    pid_orcid_cache: dict[str, str] | None = None,
    target_author_ids: set[str] | None = None,
    progress_label: str | None = None,
) -> None:
    pid_orcid_cache = pid_orcid_cache if pid_orcid_cache is not None else {}
    resolved_from_pid = 0

    author_items = [
        (author_id, authors[author_id])
        for author_id in sorted(authors)
        if (
            (target_author_ids is None or author_id in target_author_ids)
            and not authors[author_id]["orcid"]
            and authors[author_id]["dblp_pid"]
        )
    ]

    iterator = author_items
    if tqdm is not None and author_items:
        iterator = tqdm(
            author_items,
            desc=progress_label or "DBLP pid->ORCID",
            unit="author",
            leave=False,
        )

    for author_id, author in iterator:
        dblp_pid = author["dblp_pid"]
        if dblp_pid not in pid_orcid_cache:
            try:
                pid_orcid_cache[dblp_pid] = lookup_pid_orcid(client, dblp_pid)
            except FileNotFoundError:
                pid_orcid_cache[dblp_pid] = ""
            except Exception as exc:
                print(f"[orcid] pid lookup failed for {dblp_pid}: {exc}")
                pid_orcid_cache[dblp_pid] = ""

        resolved_orcid = pid_orcid_cache[dblp_pid]
        if resolved_orcid:
            authors[author_id]["orcid"] = resolved_orcid
            resolved_from_pid += 1

    for paper in papers_to_update:
        paper.author_orcids = [
            authors.get(author_id, {}).get("orcid", "") or paper_orcid
            for author_id, paper_orcid in zip(paper.author_ids, paper.paper_xml_author_orcids)
        ]

    if resolved_from_pid:
        print(f"[orcid] filled {resolved_from_pid} missing author ORCIDs from DBLP person records")


def build_author_rows(authors: dict[str, dict]) -> list[dict]:
    author_rows = []
    for author in sorted(authors.values(), key=lambda row: (-len(row["paper_ids"]), row["name"])):
        author_rows.append(
            {
                "author_id": author["author_id"],
                "name": author["name"],
                "dblp_pid": author["dblp_pid"],
                "orcid": author["orcid"],
                "paper_count": len(author["paper_ids"]),
                "paper_ids": "|".join(author["paper_ids"]),
                "venues": "|".join(sorted(author["venues"])),
                "years": "|".join(str(year) for year in sorted(author["years"])),
            }
        )
    return author_rows


def build_paper_rows(papers: list[PaperRecord]) -> list[dict]:
    return [
        {
            "paper_id": paper.paper_id,
            "dblp_key": paper.dblp_key,
            "title": paper.title,
            "year": paper.year,
            "venue_key": paper.venue_key,
            "venue_name": paper.venue_name,
            "booktitle": paper.booktitle,
            "ee": paper.ee,
            "pages": paper.pages,
            "crossref": paper.crossref,
            "dblp_url": paper.dblp_url,
            "toc_url": paper.toc_url,
            "author_ids": "|".join(paper.author_ids),
            "author_names": "|".join(paper.author_names),
            "paper_xml_author_orcids": "|".join(paper.paper_xml_author_orcids),
            "author_orcids": "|".join(paper.author_orcids),
            "author_count": len(paper.author_ids),
        }
        for paper in papers
    ]


def build_summary(
    args: argparse.Namespace,
    paper_rows: list[dict],
    author_rows: list[dict],
    edge_rows: list[dict],
    venue_counts: Counter,
) -> dict:
    authors_without_orcid = sum(1 for row in author_rows if not row.get("orcid", ""))
    return {
        "generated_at": datetime.now().isoformat(),
        "source": "DBLP proceedings XML",
        "dblp_base_url": DBLP_BASE_URL,
        "venues": {venue: DEFAULT_VENUES[venue] for venue in args.venues},
        "year_range": {"start": args.start_year, "end": args.end_year},
        "counts": {
            "papers": len(paper_rows),
            "authors": len(author_rows),
            "edges": len(edge_rows),
            "authors_without_orcid": authors_without_orcid,
        },
        "paper_counts_by_venue": dict(venue_counts),
        "notes": [
            "DBLP venue streams provide metadata only; paper-level records are fetched from per-year proceedings TOC XML.",
            "Author identity uses DBLP pid when available, otherwise a normalized name key.",
            "ORCID resolution prefers paper-level DBLP author signatures and falls back to DBLP person records via pid lookups.",
            "The output graph is an undirected weighted coauthor graph where edge weight equals the number of qualifying papers.",
        ],
    }


def progress_path(output_dir: Path) -> Path:
    return output_dir / "progress.json"


def serialize_progress(completed_years: set[tuple[str, int]]) -> dict:
    return {
        "completed_years": [
            {"venue_key": venue_key, "year": year}
            for venue_key, year in sorted(completed_years)
        ]
    }


def load_completed_years(output_dir: Path) -> set[tuple[str, int]]:
    path = progress_path(output_dir)
    if not path.exists():
        return set()

    payload = json.loads(path.read_text(encoding="utf-8"))
    completed_years = set()
    for item in payload.get("completed_years", []):
        venue_key = str(item.get("venue_key", "")).strip().lower()
        year = item.get("year")
        if venue_key and isinstance(year, int):
            completed_years.add((venue_key, year))
    return completed_years


def save_progress(output_dir: Path, completed_years: set[tuple[str, int]]) -> None:
    progress_path(output_dir).write_text(
        json.dumps(serialize_progress(completed_years), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_existing_state(
    output_dir: Path,
) -> tuple[list[PaperRecord], dict[str, dict], Counter]:
    papers_path = output_dir / "papers.csv"
    authors_path = output_dir / "authors.csv"

    if not papers_path.exists() or not authors_path.exists():
        return [], {}, Counter()

    papers: list[PaperRecord] = []
    with papers_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            papers.append(
                PaperRecord(
                    paper_id=row["paper_id"],
                    dblp_key=row["dblp_key"],
                    title=row["title"],
                    year=int(row["year"]),
                    venue_key=row["venue_key"],
                    venue_name=row["venue_name"],
                    booktitle=row["booktitle"],
                    ee=row["ee"],
                    pages=row["pages"],
                    crossref=row["crossref"],
                    dblp_url=row["dblp_url"],
                    toc_url=row["toc_url"],
                    author_ids=[item for item in row["author_ids"].split("|") if item],
                    author_names=[item for item in row["author_names"].split("|") if item],
                    paper_xml_author_orcids=row.get("paper_xml_author_orcids", "").split("|")
                    if row.get("paper_xml_author_orcids")
                    else [],
                    author_orcids=row.get("author_orcids", "").split("|")
                    if row.get("author_orcids")
                    else [],
                )
            )

    authors: dict[str, dict] = {}
    with authors_path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            authors[row["author_id"]] = {
                "author_id": row["author_id"],
                "name": row["name"],
                "dblp_pid": row.get("dblp_pid", ""),
                "orcid": row.get("orcid", ""),
                "paper_ids": [item for item in row.get("paper_ids", "").split("|") if item],
                "venues": set(item for item in row.get("venues", "").split("|") if item),
                "years": set(
                    int(item) for item in row.get("years", "").split("|") if item.isdigit()
                ),
            }

    venue_counts: Counter = Counter()
    for paper in papers:
        venue_counts[paper.venue_name] += 1

    return papers, authors, venue_counts


def persist_outputs(
    output_dir: Path,
    args: argparse.Namespace,
    papers: list[PaperRecord],
    authors: dict[str, dict],
    venue_counts: Counter,
    completed_years: set[tuple[str, int]] | None = None,
) -> dict:
    edge_rows = build_edge_rows(papers)
    author_rows = build_author_rows(authors)
    paper_rows = build_paper_rows(papers)

    write_csv(
        output_dir / "papers.csv",
        paper_rows,
        [
            "paper_id",
            "dblp_key",
            "title",
            "year",
            "venue_key",
            "venue_name",
            "booktitle",
            "ee",
            "pages",
            "crossref",
            "dblp_url",
            "toc_url",
            "author_ids",
            "author_names",
            "paper_xml_author_orcids",
            "author_orcids",
            "author_count",
        ],
    )
    write_csv(
        output_dir / "authors.csv",
        author_rows,
        [
            "author_id",
            "name",
            "dblp_pid",
            "orcid",
            "paper_count",
            "paper_ids",
            "venues",
            "years",
        ],
    )
    write_csv(
        output_dir / "edges.csv",
        edge_rows,
        [
            "source_author_id",
            "target_author_id",
            "weight",
            "paper_count",
            "paper_ids",
        ],
    )
    write_graphml(output_dir / "graph.graphml", author_rows, edge_rows)

    summary = build_summary(args, paper_rows, author_rows, edge_rows, venue_counts)
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if completed_years is not None:
        save_progress(output_dir, completed_years)
    return summary


def collect_dataset(
    args: argparse.Namespace,
    output_dir: Path | None = None,
) -> tuple[list[PaperRecord], dict[str, dict], Counter]:
    client = DblpClient(
        sleep_seconds=args.sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
    )
    orcid_client = DblpClient(
        sleep_seconds=args.orcid_sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
    )
    if output_dir is not None:
        papers, authors, venue_counts = load_existing_state(output_dir)
        completed_years = load_completed_years(output_dir)
    else:
        papers, authors, venue_counts = [], {}, Counter()
        completed_years = set()
    pid_orcid_cache: dict[str, str] = {}

    for venue_key in args.venues:
        venue_name = DEFAULT_VENUES.get(venue_key, venue_key.upper())
        print(f"[venue] {venue_name} ({venue_key})")
        for year in range(args.start_year, args.end_year + 1):
            if (venue_key, year) in completed_years:
                print(f"  - {year}: already completed, skipping")
                continue

            toc_url = extract_toc_url(client, venue_key, year)
            if not toc_url:
                print(f"  - {year}: proceedings not found, skipping")
                continue

            print(f"  - {year}: fetching {toc_url}")
            year_papers = list(iter_papers_from_toc(client, toc_url, venue_key, venue_name, year))
            print(f"    found {len(year_papers)} papers")
            papers.extend(year_papers)
            venue_counts[venue_name] += len(year_papers)
            current_year_author_ids: set[str] = set()

            for paper in year_papers:
                for author_id, author_name, author_orcid in zip(
                    paper.author_ids,
                    paper.author_names,
                    paper.paper_xml_author_orcids,
                ):
                    current_year_author_ids.add(author_id)
                    author = authors.setdefault(
                        author_id,
                        {
                            "author_id": author_id,
                            "name": author_name,
                            "dblp_pid": author_id.removeprefix("pid:")
                            if author_id.startswith("pid:")
                            else "",
                            "orcid": author_orcid,
                            "paper_ids": [],
                            "venues": set(),
                            "years": set(),
                        },
                    )
                    if author_orcid and not author["orcid"]:
                        author["orcid"] = author_orcid
                    author["paper_ids"].append(paper.paper_id)
                    author["venues"].add(paper.venue_name)
                    author["years"].add(paper.year)

            resolve_author_orcids(
                orcid_client,
                authors,
                year_papers,
                pid_orcid_cache=pid_orcid_cache,
                target_author_ids=current_year_author_ids,
                progress_label=f"{venue_name} {year} ORCID",
            )
            completed_years.add((venue_key, year))
            if output_dir is not None:
                summary = persist_outputs(
                    output_dir,
                    args,
                    papers,
                    authors,
                    venue_counts,
                    completed_years=completed_years,
                )
                print(
                    f"    checkpoint saved after {venue_name} {year}: "
                    f"{summary['counts']['papers']} papers, "
                    f"{summary['counts']['authors']} authors, "
                    f"{summary['counts']['edges']} edges, "
                    f"{summary['counts']['authors_without_orcid']} authors without ORCID"
                )

    return papers, authors, venue_counts


def build_edge_rows(papers: list[PaperRecord]) -> list[dict]:
    edge_weights: defaultdict[tuple[str, str], int] = defaultdict(int)
    edge_papers: defaultdict[tuple[str, str], list[str]] = defaultdict(list)

    for paper in papers:
        unique_authors = list(dict.fromkeys(paper.author_ids))
        for left, right in itertools.combinations(sorted(unique_authors), 2):
            edge_weights[(left, right)] += 1
            edge_papers[(left, right)].append(paper.paper_id)

    return [
        {
            "source_author_id": source,
            "target_author_id": target,
            "weight": weight,
            "paper_count": weight,
            "paper_ids": "|".join(edge_papers[(source, target)]),
        }
        for (source, target), weight in sorted(edge_weights.items())
    ]


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_graphml(path: Path, author_rows: list[dict], edge_rows: list[dict]) -> None:
    node_lines = []
    for row in author_rows:
        node_lines.append(
            (
                f'    <node id="{graphml_escape(row["author_id"])}">\n'
                f'      <data key="name">{graphml_escape(row["name"])}</data>\n'
                f'      <data key="dblp_pid">{graphml_escape(row["dblp_pid"])}</data>\n'
                f'      <data key="orcid">{graphml_escape(row["orcid"])}</data>\n'
                f'      <data key="paper_count">{row["paper_count"]}</data>\n'
                f'      <data key="venues">{graphml_escape(row["venues"])}</data>\n'
                f'      <data key="years">{graphml_escape(row["years"])}</data>\n'
                "    </node>"
            )
        )

    edge_lines = []
    for index, row in enumerate(edge_rows):
        edge_lines.append(
            (
                f'    <edge id="e{index}" '
                f'source="{graphml_escape(row["source_author_id"])}" '
                f'target="{graphml_escape(row["target_author_id"])}">\n'
                f'      <data key="weight">{row["weight"]}</data>\n'
                f'      <data key="paper_ids">{graphml_escape(row["paper_ids"])}</data>\n'
                "    </edge>"
            )
        )

    graphml = "\n".join(
        [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<graphml xmlns="http://graphml.graphdrawing.org/xmlns"',
            '         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
            '         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns',
            '         http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">',
            '  <key id="name" for="node" attr.name="name" attr.type="string"/>',
            '  <key id="dblp_pid" for="node" attr.name="dblp_pid" attr.type="string"/>',
            '  <key id="orcid" for="node" attr.name="orcid" attr.type="string"/>',
            '  <key id="paper_count" for="node" attr.name="paper_count" attr.type="int"/>',
            '  <key id="venues" for="node" attr.name="venues" attr.type="string"/>',
            '  <key id="years" for="node" attr.name="years" attr.type="string"/>',
            '  <key id="weight" for="edge" attr.name="weight" attr.type="int"/>',
            '  <key id="paper_ids" for="edge" attr.name="paper_ids" attr.type="string"/>',
            '  <graph id="G" edgedefault="undirected">',
            *node_lines,
            *edge_lines,
            "  </graph>",
            "</graphml>",
            "",
        ]
    )
    path.write_text(graphml, encoding="utf-8")


def main() -> None:
    args = parse_args()
    if args.start_year > args.end_year:
        raise SystemExit("--start-year must be <= --end-year")

    args.venues = [venue.lower() for venue in args.venues]
    invalid = [venue for venue in args.venues if venue not in DEFAULT_VENUES]
    if invalid:
        raise SystemExit(
            f"Unsupported venues: {', '.join(invalid)}. "
            f"Supported: {', '.join(DEFAULT_VENUES)}"
        )

    output_dir = args.output_dir or Path(
        f"data/dblp_ai_authors_{args.start_year}_{args.end_year}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    papers, authors, venue_counts = collect_dataset(args, output_dir=output_dir)
    summary = persist_outputs(output_dir, args, papers, authors, venue_counts)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print(f"[orcid] authors without ORCID: {summary['counts']['authors_without_orcid']}")


if __name__ == "__main__":
    main()
