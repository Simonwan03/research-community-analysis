"""Configuration for building an internal citation graph from DBLP papers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class ApiConfig:
    """Semantic Scholar API settings."""

    base_url: str = "https://api.semanticscholar.org/graph/v1"
    api_key_env_var: str = "S2_API_KEY"
    timeout_seconds: float = 60.0
    request_interval_seconds: float = 1.0
    max_retries: int = 6
    backoff_base_seconds: float = 2.0
    search_limit: int = 10
    reference_page_size: int = 1000


@dataclass(frozen=True)
class MatchConfig:
    """Thresholds and weights used during DBLP -> Semantic Scholar matching."""

    title_weight: float = 0.72
    year_weight: float = 0.18
    author_weight: float = 0.10
    high_confidence_threshold: float = 0.90
    medium_confidence_threshold: float = 0.78
    ambiguous_gap: float = 0.04
    strict_title_similarity: float = 0.94
    near_year_delta: int = 1


@dataclass(frozen=True)
class PathConfig:
    """Default input, output, and cache locations."""

    default_input_path: Path = Path("data/dblp_ai_authors_2015_2025/papers.csv")
    output_dir: Path = Path("paper_citation_graph/outputs")
    cache_dir: Path = Path(".cache/semantic_scholar")

    resolved_papers_name: str = "resolved_papers.jsonl"
    unmatched_papers_name: str = "unmatched_papers.jsonl"
    paper_references_name: str = "paper_references.jsonl"
    internal_edges_name: str = "internal_citation_edges.csv"
    graphml_name: str = "paper_graph.graphml"
    summary_name: str = "summary.json"


@dataclass(frozen=True)
class FieldConfig:
    """Fields requested from Semantic Scholar."""

    paper_fields: tuple[str, ...] = (
        "paperId",
        "title",
        "year",
        "authors",
        "externalIds",
        "venue",
        "url",
        "citationCount",
        "referenceCount",
    )
    reference_fields: tuple[str, ...] = (
        "citedPaper.paperId",
        "citedPaper.title",
        "citedPaper.year",
        "citedPaper.authors",
        "citedPaper.externalIds",
        "citedPaper.venue",
        "citedPaper.url",
        "citedPaper.citationCount",
        "citedPaper.referenceCount",
    )


@dataclass(frozen=True)
class PipelineConfig:
    """Top-level pipeline configuration."""

    api: ApiConfig = field(default_factory=ApiConfig)
    match: MatchConfig = field(default_factory=MatchConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    fields: FieldConfig = field(default_factory=FieldConfig)
