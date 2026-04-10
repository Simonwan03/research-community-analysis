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
        default=1.2,
        help="Delay between successful DBLP requests. Default: 1.2",
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


def author_identity(author_elem: ET.Element) -> tuple[str, str, str]:
    name = normalized_text(author_elem.text)
    pid = author_elem.attrib.get("pid", "").strip()
    author_id = f"pid:{pid}" if pid else f"name:{name.casefold()}"
    return author_id, name, pid


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
        for author_elem in entry.findall("author"):
            author_id, author_name, _ = author_identity(author_elem)
            authors.append(author_id)
            author_names.append(author_name)

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
        )


def collect_dataset(args: argparse.Namespace) -> tuple[list[PaperRecord], dict[str, dict], Counter]:
    client = DblpClient(
        sleep_seconds=args.sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
    )

    papers: list[PaperRecord] = []
    authors: dict[str, dict] = {}
    venue_counts: Counter = Counter()

    for venue_key in args.venues:
        venue_name = DEFAULT_VENUES.get(venue_key, venue_key.upper())
        print(f"[venue] {venue_name} ({venue_key})")
        for year in range(args.start_year, args.end_year + 1):
            toc_url = extract_toc_url(client, venue_key, year)
            if not toc_url:
                print(f"  - {year}: proceedings not found, skipping")
                continue

            print(f"  - {year}: fetching {toc_url}")
            year_papers = list(iter_papers_from_toc(client, toc_url, venue_key, venue_name, year))
            print(f"    found {len(year_papers)} papers")
            papers.extend(year_papers)
            venue_counts[venue_name] += len(year_papers)

            for paper in year_papers:
                for author_id, author_name in zip(paper.author_ids, paper.author_names):
                    author = authors.setdefault(
                        author_id,
                        {
                            "author_id": author_id,
                            "name": author_name,
                            "dblp_pid": author_id.removeprefix("pid:")
                            if author_id.startswith("pid:")
                            else "",
                            "paper_ids": [],
                            "venues": set(),
                            "years": set(),
                        },
                    )
                    author["paper_ids"].append(paper.paper_id)
                    author["venues"].add(paper.venue_name)
                    author["years"].add(paper.year)

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

    papers, authors, venue_counts = collect_dataset(args)
    edge_rows = build_edge_rows(papers)

    author_rows = []
    for author in sorted(authors.values(), key=lambda row: (-len(row["paper_ids"]), row["name"])):
        author_rows.append(
            {
                "author_id": author["author_id"],
                "name": author["name"],
                "dblp_pid": author["dblp_pid"],
                "paper_count": len(author["paper_ids"]),
                "paper_ids": "|".join(author["paper_ids"]),
                "venues": "|".join(sorted(author["venues"])),
                "years": "|".join(str(year) for year in sorted(author["years"])),
            }
        )

    paper_rows = [
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
            "author_count": len(paper.author_ids),
        }
        for paper in papers
    ]

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

    summary = {
        "generated_at": datetime.now().isoformat(),
        "source": "DBLP proceedings XML",
        "dblp_base_url": DBLP_BASE_URL,
        "venues": {venue: DEFAULT_VENUES[venue] for venue in args.venues},
        "year_range": {"start": args.start_year, "end": args.end_year},
        "counts": {
            "papers": len(paper_rows),
            "authors": len(author_rows),
            "edges": len(edge_rows),
        },
        "paper_counts_by_venue": dict(venue_counts),
        "notes": [
            "DBLP venue streams provide metadata only; paper-level records are fetched from per-year proceedings TOC XML.",
            "Author identity uses DBLP pid when available, otherwise a normalized name key.",
            "The output graph is an undirected weighted coauthor graph where edge weight equals the number of qualifying papers.",
        ],
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
