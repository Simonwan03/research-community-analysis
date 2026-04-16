"""Matching logic for resolving local DBLP-derived papers to Semantic Scholar papers."""

from __future__ import annotations

import csv
import json
import logging
import re
import unicodedata
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from config import MatchConfig
from semantic_scholar_client import SemanticScholarClient

LOGGER = logging.getLogger(__name__)

try:
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover - optional dependency
    fuzz = None


@dataclass
class LocalPaper:
    """A local DBLP-derived paper record normalized for matching."""

    local_id: str
    title: str
    year: int | None = None
    authors: list[str] | None = None
    doi: str | None = None
    dblp_key: str | None = None
    paper_url: str | None = None
    venue: str | None = None
    semantic_scholar_paper_id: str | None = None
    raw: dict[str, Any] | None = None


@dataclass
class MatchResult:
    """Resolution result for one local paper."""

    local_input_id: str
    local_title: str
    local_year: int | None
    matched_paper_id: str | None
    matched_title: str | None
    matched_year: int | None
    match_score: float
    match_status: str
    doi: str | None = None
    dblp_key: str | None = None
    reason: str | None = None
    candidate_count: int = 0
    second_best_score: float | None = None


def normalize_title(value: str | None) -> str:
    """Normalize titles for stable fuzzy matching."""

    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_author_name(value: str | None) -> str:
    """Normalize author names for overlap checks."""

    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def title_similarity(left: str | None, right: str | None) -> float:
    """Return a normalized title similarity in [0, 1]."""

    left_norm = normalize_title(left)
    right_norm = normalize_title(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 1.0
    if fuzz is not None:
        return float(fuzz.token_set_ratio(left_norm, right_norm)) / 100.0

    sequence_score = SequenceMatcher(None, left_norm, right_norm).ratio()
    left_tokens = set(left_norm.split())
    right_tokens = set(right_norm.split())
    token_score = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    return (sequence_score + token_score) / 2.0


def year_score(local_year: int | None, candidate_year: int | None, near_year_delta: int) -> float:
    """Score year compatibility."""

    if local_year is None or candidate_year is None:
        return 0.5
    delta = abs(local_year - candidate_year)
    if delta == 0:
        return 1.0
    if delta <= near_year_delta:
        return 0.75
    if delta <= 2:
        return 0.45
    return 0.0


def author_overlap(local_authors: list[str] | None, candidate_authors: list[dict[str, Any]] | None) -> float:
    """Estimate author overlap using normalized full names and last names."""

    if not local_authors or not candidate_authors:
        return 0.0

    local_names = {normalize_author_name(name) for name in local_authors if normalize_author_name(name)}
    candidate_names = {
        normalize_author_name(author.get("name"))
        for author in candidate_authors
        if normalize_author_name(author.get("name"))
    }
    if not local_names or not candidate_names:
        return 0.0

    exact_overlap = len(local_names & candidate_names) / max(1, min(len(local_names), len(candidate_names)))
    local_last = {name.split()[-1] for name in local_names if name.split()}
    candidate_last = {name.split()[-1] for name in candidate_names if name.split()}
    last_overlap = len(local_last & candidate_last) / max(1, min(len(local_last), len(candidate_last)))
    return max(exact_overlap, 0.7 * last_overlap)


def candidate_score(local: LocalPaper, candidate: dict[str, Any], config: MatchConfig) -> float:
    """Score a Semantic Scholar candidate against one local paper."""

    title_part = title_similarity(local.title, candidate.get("title"))
    year_part = year_score(local.year, candidate.get("year"), config.near_year_delta)
    author_part = author_overlap(local.authors, candidate.get("authors"))
    return (
        config.title_weight * title_part
        + config.year_weight * year_part
        + config.author_weight * author_part
    )


def resolve_paper(
    local: LocalPaper,
    client: SemanticScholarClient,
    config: MatchConfig,
    paper_fields: tuple[str, ...],
    search_limit: int,
    min_match_score: float | None = None,
) -> tuple[MatchResult, dict[str, Any] | None]:
    """Resolve one local paper to Semantic Scholar metadata."""

    if local.semantic_scholar_paper_id:
        paper = client.get_paper(local.semantic_scholar_paper_id, fields=paper_fields)
        if paper:
            return _result(local, paper, 1.0, "cached_paper_id", "matched existing Semantic Scholar paperId"), paper

    if local.doi:
        paper = client.get_paper_by_doi(local.doi, fields=paper_fields)
        if paper:
            return _result(local, paper, 1.0, "exact_doi", "resolved by DOI"), paper

    if not local.title:
        return _unmatched(local, "not_found", "missing local title"), None

    candidates = client.search_papers(local.title, year=local.year, limit=search_limit)
    if not candidates:
        candidates = client.search_papers(local.title, year=None, limit=search_limit)

    if not candidates:
        return _unmatched(local, "not_found", "Semantic Scholar search returned no candidates"), None

    scored = sorted(
        ((candidate_score(local, candidate, config), candidate) for candidate in candidates),
        key=lambda item: (-item[0], item[1].get("paperId") or ""),
    )
    best_score, best = scored[0]
    second_best = scored[1][0] if len(scored) > 1 else None
    threshold = min_match_score if min_match_score is not None else config.medium_confidence_threshold

    title_part = title_similarity(local.title, best.get("title"))
    exact_or_near_year = year_score(local.year, best.get("year"), config.near_year_delta) >= 0.75
    close_second = second_best is not None and best_score - second_best <= config.ambiguous_gap

    if best_score < threshold:
        return _unmatched(
            local,
            "not_found",
            f"best candidate score {best_score:.3f} below threshold {threshold:.3f}",
            candidate_count=len(candidates),
            second_best_score=second_best,
        ), None

    if close_second and best_score < config.high_confidence_threshold:
        return _unmatched(
            local,
            "ambiguous",
            "top two candidates are too close",
            matched=best,
            score=best_score,
            candidate_count=len(candidates),
            second_best_score=second_best,
        ), None

    if title_part >= config.strict_title_similarity and exact_or_near_year:
        status = "high_confidence_title"
    elif best_score >= config.high_confidence_threshold:
        status = "high_confidence_title"
    else:
        status = "medium_confidence_title"

    return _result(
        local,
        best,
        best_score,
        status,
        "selected best title-search candidate",
        candidate_count=len(candidates),
        second_best_score=second_best,
    ), best


def load_local_papers(path: Path) -> list[LocalPaper]:
    """Load local paper records from CSV or JSONL."""

    if path.suffix.lower() == ".csv":
        rows = _read_csv(path)
    elif path.suffix.lower() in {".jsonl", ".ndjson"}:
        rows = _read_jsonl(path)
    else:
        raise ValueError(f"Unsupported input format: {path}. Use .csv or .jsonl")

    return [local_paper_from_record(row, index) for index, row in enumerate(rows)]


def local_paper_from_record(record: dict[str, Any], index: int) -> LocalPaper:
    """Create a LocalPaper from a flexible DBLP-derived record."""

    title = _first(record, "title", "paper_title", "name")
    year = _to_int(_first(record, "year", "publication_year"))
    dblp_key = _first(record, "dblp_key", "key")
    doi = _normalize_doi(_first(record, "doi", "DOI"))
    if not doi:
        doi = _extract_doi(_first(record, "ee", "paper_url", "url", "dblp_url"))
    semantic_id = _first(record, "semantic_scholar_paper_id", "semanticScholarPaperId", "paperId")
    paper_url = _first(record, "paper_url", "url", "dblp_url", "ee")
    venue = _first(record, "venue", "venue_name", "booktitle")
    authors = _parse_authors(_first(record, "authors", "author_names", "author_list"))
    local_id = _first(record, "paper_id", "local_id", "id", "dblp_key", "key") or f"row:{index}"

    return LocalPaper(
        local_id=str(local_id),
        title=str(title or ""),
        year=year,
        authors=authors,
        doi=doi,
        dblp_key=str(dblp_key) if dblp_key else None,
        paper_url=str(paper_url) if paper_url else None,
        venue=str(venue) if venue else None,
        semantic_scholar_paper_id=str(semantic_id) if semantic_id else None,
        raw=record,
    )


def match_result_to_json(result: MatchResult) -> dict[str, Any]:
    """Convert MatchResult to a JSON-serializable dict."""

    return asdict(result)


def _result(
    local: LocalPaper,
    paper: dict[str, Any],
    score: float,
    status: str,
    reason: str,
    candidate_count: int = 1,
    second_best_score: float | None = None,
) -> MatchResult:
    return MatchResult(
        local_input_id=local.local_id,
        local_title=local.title,
        local_year=local.year,
        matched_paper_id=paper.get("paperId"),
        matched_title=paper.get("title"),
        matched_year=paper.get("year"),
        match_score=round(float(score), 6),
        match_status=status,
        doi=local.doi,
        dblp_key=local.dblp_key,
        reason=reason,
        candidate_count=candidate_count,
        second_best_score=round(float(second_best_score), 6) if second_best_score is not None else None,
    )


def _unmatched(
    local: LocalPaper,
    status: str,
    reason: str,
    matched: dict[str, Any] | None = None,
    score: float = 0.0,
    candidate_count: int = 0,
    second_best_score: float | None = None,
) -> MatchResult:
    return MatchResult(
        local_input_id=local.local_id,
        local_title=local.title,
        local_year=local.year,
        matched_paper_id=matched.get("paperId") if matched else None,
        matched_title=matched.get("title") if matched else None,
        matched_year=matched.get("year") if matched else None,
        match_score=round(float(score), 6),
        match_status=status,
        doi=local.doi,
        dblp_key=local.dblp_key,
        reason=reason,
        candidate_count=candidate_count,
        second_best_score=round(float(second_best_score), 6) if second_best_score is not None else None,
    )


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON on line {line_number} of {path}") from exc
    return rows


def _first(record: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = record.get(name)
        if value not in (None, ""):
            return value
    return None


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_authors(value: Any) -> list[str] | None:
    if value is None or value == "":
        return None
    if isinstance(value, list):
        names = [author.get("name", "") if isinstance(author, dict) else str(author) for author in value]
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("["):
            try:
                parsed = json.loads(stripped)
                return _parse_authors(parsed)
            except json.JSONDecodeError:
                pass
        separator = "|" if "|" in stripped else ";"
        names = [part.strip() for part in stripped.split(separator)]
    else:
        names = [str(value)]
    return [name for name in names if name]


def _normalize_doi(value: Any) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^doi:", "", text, flags=re.IGNORECASE)
    return text.strip() or None


def _extract_doi(value: Any) -> str | None:
    if not value:
        return None
    text = str(value)
    match = re.search(r"(10\.\d{4,9}/[^\s\"<>]+)", text, flags=re.IGNORECASE)
    return _normalize_doi(match.group(1)) if match else None

