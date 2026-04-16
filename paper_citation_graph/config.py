"""Configuration for the Semantic Scholar paper citation graph pipeline."""

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
    batch_size: int = 100
    max_workers: int = 4


@dataclass(frozen=True)
class MatchConfig:
    """Paper matching thresholds and feature weights."""

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
    """Default input, output, and cache paths."""

    default_input_path: Path = Path("data/dblp_papers.jsonl")
    output_dir: Path = Path("outputs")
    cache_dir: Path = Path(".cache/semantic_scholar")

    resolved_papers_name: str = "resolved_papers.jsonl"
    unmatched_papers_name: str = "unmatched_papers.jsonl"
    paper_metadata_name: str = "paper_metadata.jsonl"
    paper_references_name: str = "paper_references.jsonl"
    graphml_name: str = "paper_graph.graphml"
    gpickle_name: str = "paper_graph.gpickle"
    summary_name: str = "summary.json"


@dataclass(frozen=True)
class FieldConfig:
    """Semantic Scholar fields requested by the pipeline."""

    search_fields: tuple[str, ...] = (
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
    """All configuration used by the pipeline."""

    api: ApiConfig = field(default_factory=ApiConfig)
    match: MatchConfig = field(default_factory=MatchConfig)
    paths: PathConfig = field(default_factory=PathConfig)
    fields: FieldConfig = field(default_factory=FieldConfig)
