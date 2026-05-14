"""Resolve local DBLP paper rows to Semantic Scholar paper IDs."""

from __future__ import annotations

import csv
import json
import re
import unicodedata
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from config import MatchConfig
from semantic_scholar_client import SemanticScholarClient

try:
    from rapidfuzz import fuzz
except ImportError:  # pragma: no cover
    fuzz = None


@dataclass
class LocalPaper:
    """Local paper metadata used for matching and final graph export."""

    local_id: str
    title: str
    year: int | None = None
    authors: list[str] | None = None
    doi: str | None = None
    dblp_key: str | None = None
    ee: str | None = None
    dblp_url: str | None = None
    venue: str | None = None
    raw: dict[str, Any] | None = None


@dataclass
class MatchResult:
    """Stored resolution result for one local paper."""

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


def load_local_papers(path: Path) -> list[LocalPaper]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return [local_paper_from_record(row, index) for index, row in enumerate(rows)]


def local_paper_from_record(record: dict[str, Any], index: int) -> LocalPaper:
    title = str(record.get("title") or "")
    local_id = str(record.get("paper_id") or record.get("dblp_key") or f"row:{index}")
    return LocalPaper(
        local_id=local_id,
        title=title,
        year=_to_int(record.get("year")),
        authors=_parse_authors(record.get("author_names")),
        doi=_extract_doi(record.get("ee")),
        dblp_key=str(record.get("dblp_key") or "") or None,
        ee=str(record.get("ee") or "") or None,
        dblp_url=str(record.get("dblp_url") or "") or None,
        venue=str(record.get("venue_name") or record.get("booktitle") or "") or None,
        raw=record,
    )


def resolve_paper(
    local: LocalPaper,
    client: SemanticScholarClient,
    config: MatchConfig,
    paper_fields: tuple[str, ...],
    search_limit: int,
    min_match_score: float | None = None,
) -> tuple[MatchResult, dict[str, Any] | None]:
    if local.doi:
        paper = client.get_paper_by_doi(local.doi, fields=paper_fields)
        if paper:
            return _result(local, paper, 1.0, "exact_doi", "resolved by DOI"), paper

    if not local.title:
        return _unmatched(local, "not_found", "missing local title"), None

    candidates = client.search_papers(local.title, year=None, limit=search_limit)
    if not candidates:
        return _unmatched(local, "not_found", "Semantic Scholar search returned no candidates"), None

    scored = sorted(
        ((candidate_score(local, candidate, config), candidate) for candidate in candidates),
        key=lambda item: (-item[0], item[1].get("paperId") or ""),
    )
    best_score, best = scored[0]
    second_best_score = scored[1][0] if len(scored) > 1 else None
    threshold = min_match_score if min_match_score is not None else config.medium_confidence_threshold
    title_part = title_similarity(local.title, best.get("title"))
    exact_or_near_year = year_score(local.year, best.get("year"), config.near_year_delta) >= 0.75
    close_second = second_best_score is not None and best_score - second_best_score <= config.ambiguous_gap

    if best_score < threshold:
        return _unmatched(
            local,
            "not_found",
            f"best candidate score {best_score:.3f} below threshold {threshold:.3f}",
            candidate_count=len(candidates),
            second_best_score=second_best_score,
        ), None
    if close_second and best_score < config.high_confidence_threshold:
        return _unmatched(
            local,
            "ambiguous",
            "top two candidates are too close",
            matched=best,
            score=best_score,
            candidate_count=len(candidates),
            second_best_score=second_best_score,
        ), None

    status = "high_confidence_title" if title_part >= config.strict_title_similarity and exact_or_near_year else "medium_confidence_title"
    if best_score >= config.high_confidence_threshold:
        status = "high_confidence_title"
    return _result(
        local,
        best,
        best_score,
        status,
        "selected best title-search candidate",
        candidate_count=len(candidates),
        second_best_score=second_best_score,
    ), best


def match_result_to_json(result: MatchResult) -> dict[str, Any]:
    return asdict(result)


def normalize_title(value: str | None) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_author_name(value: str | None) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def title_similarity(left: str | None, right: str | None) -> float:
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
    return (
        config.title_weight * title_similarity(local.title, candidate.get("title"))
        + config.year_weight * year_score(local.year, candidate.get("year"), config.near_year_delta)
        + config.author_weight * author_overlap(local.authors, candidate.get("authors"))
    )


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


def _parse_authors(value: Any) -> list[str] | None:
    if value is None or value == "":
        return None
    text = str(value).strip()
    if not text:
        return None
    return [part.strip() for part in text.split("|") if part.strip()]


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extract_doi(value: Any) -> str | None:
    if not value:
        return None
    text = str(value)
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^doi:", "", text, flags=re.IGNORECASE)
    match = re.search(r"(10\.\d{4,9}/[^\s\"<>]+)", text, flags=re.IGNORECASE)
    return match.group(1).rstrip(".,);") if match else None
