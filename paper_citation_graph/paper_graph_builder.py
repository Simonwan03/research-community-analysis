"""Build a paper citation graph from DBLP-derived seed papers and Semantic Scholar references."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from config import ApiConfig, PipelineConfig
from graph_utils import build_citation_graph, export_graph, graph_summary
from matcher import (
    LocalPaper,
    load_local_papers,
    match_result_to_json,
    resolve_paper,
)
from semantic_scholar_client import SemanticScholarClient

LOGGER = logging.getLogger(__name__)

ACCEPTED_MATCH_STATUSES = {
    "exact_doi",
    "cached_paper_id",
    "high_confidence_title",
    "medium_confidence_title",
}


class ProgressReporter:
    """Minimal terminal progress reporter without third-party dependencies."""

    def __init__(self, label: str, total: int, enabled: bool = True, min_interval_seconds: float = 0.5):
        self.label = label
        self.total = max(total, 0)
        self.enabled = enabled and sys.stderr.isatty()
        self.min_interval_seconds = min_interval_seconds
        self.started_at = time.monotonic()
        self.last_printed_at = 0.0
        self.current = 0
        self.last_message_length = 0

    def update(self, current: int, detail: str = "", force: bool = False) -> None:
        self.current = current
        if not self.enabled:
            return
        now = time.monotonic()
        if not force and now - self.last_printed_at < self.min_interval_seconds and current < self.total:
            return
        self.last_printed_at = now
        elapsed = max(now - self.started_at, 1e-9)
        rate = current / elapsed
        percent = (current / self.total * 100.0) if self.total else 100.0
        message = f"{self.label}: {current}/{self.total} ({percent:5.1f}%) | {rate:5.2f}/s"
        if detail:
            message += f" | {detail}"
        padding = max(0, self.last_message_length - len(message))
        sys.stderr.write("\r" + message + (" " * padding))
        sys.stderr.flush()
        self.last_message_length = len(message)

    def finish(self, detail: str = "") -> None:
        self.update(self.total, detail=detail, force=True)
        if self.enabled:
            sys.stderr.write("\n")
            sys.stderr.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a directed paper citation graph from DBLP-derived papers."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=PipelineConfig().paths.default_input_path,
        help="Input DBLP-derived papers file (.jsonl or .csv).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PipelineConfig().paths.output_dir,
        help="Output directory for JSONL artifacts and graph exports.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=PipelineConfig().paths.cache_dir,
        help="Disk cache directory for Semantic Scholar API responses.",
    )
    parser.add_argument("--max-papers", type=int, default=None, help="Process at most this many seed papers.")
    parser.add_argument("--resume", action="store_true", help="Reuse existing output JSONL artifacts when possible.")
    parser.add_argument("--skip-resolution", action="store_true", help="Load resolved_papers.jsonl instead of resolving input papers.")
    parser.add_argument(
        "--fetch-references",
        action="store_true",
        default=True,
        help="Fetch references for matched papers. Enabled by default.",
    )
    parser.add_argument(
        "--no-fetch-references",
        dest="fetch_references",
        action="store_false",
        help="Build a graph from resolved seed metadata only.",
    )
    parser.add_argument(
        "--min-match-score",
        type=float,
        default=None,
        help="Override the medium-confidence matching threshold.",
    )
    parser.add_argument("--no-cache", action="store_true", help="Disable Semantic Scholar disk cache.")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=PipelineConfig().api.max_workers,
        help="Maximum concurrent Semantic Scholar request workers.",
    )
    parser.add_argument(
        "--request-interval",
        type=float,
        default=PipelineConfig().api.request_interval_seconds,
        help="Minimum seconds between request starts across all workers.",
    )
    parser.add_argument(
        "--quiet-progress",
        action="store_true",
        help="Disable terminal progress bars.",
    )
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(asctime)s %(levelname)s %(message)s")

    config = PipelineConfig()
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    client = SemanticScholarClient(
        ApiConfig(
            base_url=config.api.base_url,
            api_key_env_var=config.api.api_key_env_var,
            timeout_seconds=config.api.timeout_seconds,
            request_interval_seconds=args.request_interval,
            max_retries=config.api.max_retries,
            backoff_base_seconds=config.api.backoff_base_seconds,
            search_limit=config.api.search_limit,
            reference_page_size=config.api.reference_page_size,
            batch_size=config.api.batch_size,
            max_workers=args.max_workers,
        ),
        cache_dir=args.cache_dir,
        use_cache=not args.no_cache,
    )

    local_papers = load_local_papers(args.input)
    if args.max_papers is not None:
        local_papers = local_papers[: args.max_papers]
    LOGGER.info("Loaded %d local seed papers from %s", len(local_papers), args.input)

    paths = _output_paths(output_dir, config)
    resolved_rows, unmatched_rows = _resolve_phase(
        local_papers=local_papers,
        client=client,
        config=config,
        paths=paths,
        resume=args.resume,
        skip_resolution=args.skip_resolution,
        min_match_score=args.min_match_score,
        max_workers=args.max_workers,
        show_progress=not args.quiet_progress,
    )

    seed_metadata, reference_edges = _reference_phase(
        resolved_rows=resolved_rows,
        client=client,
        config=config,
        paths=paths,
        resume=args.resume,
        fetch_references=args.fetch_references,
        max_workers=args.max_workers,
        show_progress=not args.quiet_progress,
    )

    seed_paper_ids = {row["matched_paper_id"] for row in resolved_rows if row.get("matched_paper_id")}
    graph = build_citation_graph(seed_metadata, reference_edges)
    export_graph(graph, paths["graphml"], paths["gpickle"])

    ambiguous_count = sum(1 for row in unmatched_rows if row.get("match_status") == "ambiguous")
    unmatched_count = sum(1 for row in unmatched_rows if row.get("match_status") != "ambiguous")
    summary = graph_summary(
        graph=graph,
        seed_paper_ids=seed_paper_ids,
        seed_count=len(local_papers),
        matched_count=len(resolved_rows),
        unmatched_count=unmatched_count,
        ambiguous_count=ambiguous_count,
    )
    paths["summary"].write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    LOGGER.info("Wrote graph with %d nodes and %d edges to %s", graph.number_of_nodes(), graph.number_of_edges(), paths["graphml"])
    LOGGER.info("Wrote summary to %s", paths["summary"])


def _resolve_phase(
    local_papers: list[LocalPaper],
    client: SemanticScholarClient,
    config: PipelineConfig,
    paths: dict[str, Path],
    resume: bool,
    skip_resolution: bool,
    min_match_score: float | None,
    max_workers: int,
    show_progress: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if skip_resolution:
        resolved_rows = _read_jsonl_if_exists(paths["resolved"])
        unmatched_rows = _read_jsonl_if_exists(paths["unmatched"])
        LOGGER.info("Loaded %d resolved rows and %d unmatched rows", len(resolved_rows), len(unmatched_rows))
        return resolved_rows, unmatched_rows

    resolved_rows = _read_jsonl_if_exists(paths["resolved"]) if resume else []
    unmatched_rows = _read_jsonl_if_exists(paths["unmatched"]) if resume else []
    seen_local_ids = {row.get("local_input_id") for row in resolved_rows + unmatched_rows}

    if not resume:
        _reset_file(paths["resolved"])
        _reset_file(paths["unmatched"])

    tasks = [
        (index, local)
        for index, local in enumerate(local_papers, 1)
        if local.local_id not in seen_local_ids
    ]
    if tasks:
        LOGGER.info("Resolving %d papers with %d workers", len(tasks), max_workers)

    def resolve_task(task: tuple[int, LocalPaper]) -> tuple[int, Any]:
        index, local = task
        LOGGER.info("Resolving %d/%d: %s", index, len(local_papers), local.title[:120])
        try:
            result, _ = resolve_paper(
                local,
                client=client,
                config=config.match,
                paper_fields=config.fields.paper_fields,
                search_limit=config.api.search_limit,
                min_match_score=min_match_score,
            )
        except Exception as exc:
            LOGGER.exception("Resolution failed for %s", local.local_id)
            result = _failure_result(local, exc)
        return index, result

    progress = ProgressReporter(
        "Resolution",
        len(tasks),
        enabled=show_progress,
    )
    resolved_new = 0
    unmatched_new = 0
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        for completed, (_, result) in enumerate(executor.map(resolve_task, tasks), 1):
            row = match_result_to_json(result)
            if result.match_status in ACCEPTED_MATCH_STATUSES and result.matched_paper_id:
                resolved_rows.append(row)
                _append_jsonl(paths["resolved"], row)
                resolved_new += 1
            else:
                unmatched_rows.append(row)
                _append_jsonl(paths["unmatched"], row)
                unmatched_new += 1
            progress.update(
                completed,
                detail=f"matched={resolved_new} unmatched={unmatched_new}",
            )
    progress.finish(detail=f"matched={resolved_new} unmatched={unmatched_new}")

    return resolved_rows, unmatched_rows


def _reference_phase(
    resolved_rows: list[dict[str, Any]],
    client: SemanticScholarClient,
    config: PipelineConfig,
    paths: dict[str, Path],
    resume: bool,
    fetch_references: bool,
    max_workers: int,
    show_progress: bool,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    metadata_rows = _read_jsonl_if_exists(paths["metadata"]) if resume else []
    reference_edges = _read_jsonl_if_exists(paths["references"]) if resume else []
    metadata_by_id = {row.get("paperId"): row for row in metadata_rows if row.get("paperId")}
    completed_reference_sources = {row.get("source_paper_id") for row in reference_edges if row.get("source_paper_id")}

    if not resume:
        _reset_file(paths["metadata"])
        _reset_file(paths["references"])

    tasks = [
        (index, row)
        for index, row in enumerate(resolved_rows, 1)
        if row.get("matched_paper_id")
    ]
    if tasks:
        LOGGER.info("Fetching metadata/references for %d matched papers with %d workers", len(tasks), max_workers)

    metadata_snapshot = dict(metadata_by_id)
    completed_snapshot = set(completed_reference_sources)

    def reference_task(task: tuple[int, dict[str, Any]]) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[dict[str, Any]]]:
        index, row = task
        paper_id = row.get("matched_paper_id")
        if not paper_id:
            return None, [], []

        new_metadata_rows: list[dict[str, Any]] = []
        new_reference_edges: list[dict[str, Any]] = []
        metadata = metadata_snapshot.get(paper_id)
        if metadata is None:
            LOGGER.info("Fetching source metadata %d/%d: %s", index, len(resolved_rows), paper_id)
            metadata = client.get_paper(paper_id, fields=config.fields.paper_fields) or {"paperId": paper_id}
            metadata["source_origin"] = "seed"
            new_metadata_rows.append(metadata)

        if not fetch_references or paper_id in completed_snapshot:
            return metadata, new_metadata_rows, new_reference_edges

        LOGGER.info("Fetching references for %s", paper_id)
        try:
            references = client.get_references(paper_id, fields=config.fields.reference_fields)
        except Exception:
            LOGGER.exception("Reference fetch failed for %s", paper_id)
            return metadata, new_metadata_rows, new_reference_edges

        for reference in references:
            cited_paper = reference.get("citedPaper") or {}
            target_id = cited_paper.get("paperId")
            if not target_id:
                continue
            if target_id not in metadata_snapshot:
                cited_paper["source_origin"] = "reference_only"
                new_metadata_rows.append(cited_paper)

            edge_row = _reference_row(metadata, cited_paper)
            new_reference_edges.append(edge_row)

        return metadata, new_metadata_rows, new_reference_edges

    seed_metadata: dict[str, dict[str, Any]] = {}
    seen_metadata_ids = set(metadata_by_id)
    progress = ProgressReporter(
        "References",
        len(tasks),
        enabled=show_progress,
    )
    new_edge_count = 0
    new_metadata_count = 0
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        for completed, (metadata, new_metadata_rows, new_reference_edges) in enumerate(executor.map(reference_task, tasks), 1):
            if metadata and metadata.get("paperId"):
                seed_metadata[metadata["paperId"]] = metadata
                if metadata["paperId"] not in seen_metadata_ids:
                    metadata_by_id[metadata["paperId"]] = metadata
                    seen_metadata_ids.add(metadata["paperId"])
                    _append_jsonl(paths["metadata"], metadata)
                    new_metadata_count += 1

            for metadata_row in new_metadata_rows:
                paper_id = metadata_row.get("paperId")
                if not paper_id or paper_id in seen_metadata_ids:
                    continue
                metadata_by_id[paper_id] = metadata_row
                seen_metadata_ids.add(paper_id)
                _append_jsonl(paths["metadata"], metadata_row)
                new_metadata_count += 1

            for edge_row in new_reference_edges:
                reference_edges.append(edge_row)
                _append_jsonl(paths["references"], edge_row)
                new_edge_count += 1

            progress.update(
                completed,
                detail=f"new_edges={new_edge_count} new_metadata={new_metadata_count}",
            )
    progress.finish(detail=f"new_edges={new_edge_count} new_metadata={new_metadata_count}")

    return seed_metadata, reference_edges


def _reference_row(source: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_paper_id": source.get("paperId"),
        "target_paper_id": target.get("paperId"),
        "source_title": source.get("title"),
        "target_title": target.get("title"),
        "source_year": source.get("year"),
        "target_year": target.get("year"),
        "target_authors": target.get("authors") or [],
        "target_external_ids": target.get("externalIds") or {},
        "target_venue": target.get("venue"),
        "target_url": target.get("url"),
        "target_citation_count": target.get("citationCount"),
        "target_reference_count": target.get("referenceCount"),
        "target_metadata": target,
    }


def _failure_result(local: LocalPaper, exc: Exception) -> Any:
    from matcher import MatchResult

    return MatchResult(
        local_input_id=local.local_id,
        local_title=local.title,
        local_year=local.year,
        matched_paper_id=None,
        matched_title=None,
        matched_year=None,
        match_score=0.0,
        match_status="not_found",
        doi=local.doi,
        dblp_key=local.dblp_key,
        reason=f"resolution_error: {exc}",
    )


def _output_paths(output_dir: Path, config: PipelineConfig) -> dict[str, Path]:
    paths = config.paths
    return {
        "resolved": output_dir / paths.resolved_papers_name,
        "unmatched": output_dir / paths.unmatched_papers_name,
        "metadata": output_dir / paths.paper_metadata_name,
        "references": output_dir / paths.paper_references_name,
        "graphml": output_dir / paths.graphml_name,
        "gpickle": output_dir / paths.gpickle_name,
        "summary": output_dir / paths.summary_name,
    }


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _read_jsonl_if_exists(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _reset_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


if __name__ == "__main__":
    main()
