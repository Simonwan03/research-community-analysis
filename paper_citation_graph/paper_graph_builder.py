"""Build a paper-to-paper citation subgraph for papers listed in papers.csv."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import Any

import networkx as nx

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import ApiConfig, PipelineConfig
from graph_utils import build_internal_citation_graph, write_internal_edges_csv, write_summary
from matcher import LocalPaper, load_local_papers, match_result_to_json, resolve_paper
from semantic_scholar_client import SemanticScholarClient

LOGGER = logging.getLogger(__name__)

ACCEPTED_MATCH_STATUSES = {
    "exact_doi",
    "high_confidence_title",
    "medium_confidence_title",
}


def parse_args() -> argparse.Namespace:
    config = PipelineConfig()
    parser = argparse.ArgumentParser(
        description="Build an internal citation graph for the papers contained in a DBLP-derived papers.csv."
    )
    parser.add_argument("--input", type=Path, default=config.paths.default_input_path, help="Input papers.csv file.")
    parser.add_argument("--output-dir", type=Path, default=config.paths.output_dir, help="Output directory.")
    parser.add_argument("--cache-dir", type=Path, default=config.paths.cache_dir, help="HTTP cache directory.")
    parser.add_argument("--max-papers", type=int, default=None, help="Process at most this many local papers.")
    parser.add_argument("--request-interval", type=float, default=config.api.request_interval_seconds, help="Minimum seconds between API requests.")
    parser.add_argument("--min-match-score", type=float, default=None, help="Override the default medium-confidence match threshold.")
    parser.add_argument("--resume", action="store_true", help="Reuse existing resolution outputs when possible.")
    parser.add_argument("--skip-resolution", action="store_true", help="Load resolved_papers.jsonl instead of querying for matches again.")
    parser.add_argument("--no-cache", action="store_true", help="Disable the local HTTP cache.")
    parser.add_argument("--log-level", default="INFO", help="Python logging level.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(asctime)s %(levelname)s %(message)s")

    config = PipelineConfig()
    output_paths = output_path_map(args.output_dir, config)
    args.output_dir.mkdir(parents=True, exist_ok=True)

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
        ),
        cache_dir=args.cache_dir,
        use_cache=not args.no_cache,
    )

    local_papers = load_local_papers(args.input)
    if args.max_papers is not None:
        local_papers = local_papers[: args.max_papers]
    local_by_id = {paper.local_id: paper for paper in local_papers}
    LOGGER.info("Loaded %d local papers from %s", len(local_papers), args.input)

    resolved_rows, unmatched_rows = resolve_local_papers(
        local_papers=local_papers,
        client=client,
        config=config,
        paths=output_paths,
        resume=args.resume,
        skip_resolution=args.skip_resolution,
        min_match_score=args.min_match_score,
    )

    matched_paper_id_to_local_id = {
        row["matched_paper_id"]: row["local_input_id"]
        for row in resolved_rows
        if row.get("matched_paper_id") and row.get("match_status") in ACCEPTED_MATCH_STATUSES
    }
    internal_edges = fetch_internal_reference_edges(
        resolved_rows=resolved_rows,
        matched_paper_id_to_local_id=matched_paper_id_to_local_id,
        client=client,
        reference_fields=config.fields.reference_fields,
        references_jsonl_path=output_paths["references"],
        resume=args.resume,
    )

    graph = build_internal_citation_graph(local_by_id, resolved_rows, internal_edges)
    nx.write_graphml(graph, output_paths["graphml"])
    write_internal_edges_csv(output_paths["internal_edges"], internal_edges)

    ambiguous_count = sum(1 for row in unmatched_rows if row.get("match_status") == "ambiguous")
    unmatched_count = sum(1 for row in unmatched_rows if row.get("match_status") != "ambiguous")
    write_summary(
        output_paths["summary"],
        graph=graph,
        seed_count=len(local_papers),
        matched_count=len(resolved_rows),
        unmatched_count=unmatched_count,
        ambiguous_count=ambiguous_count,
    )

    LOGGER.info("Wrote %d internal citation edges to %s", len(internal_edges), output_paths["internal_edges"])
    LOGGER.info("Wrote graph with %d nodes and %d edges to %s", graph.number_of_nodes(), graph.number_of_edges(), output_paths["graphml"])


def resolve_local_papers(
    local_papers: list[LocalPaper],
    client: SemanticScholarClient,
    config: PipelineConfig,
    paths: dict[str, Path],
    resume: bool,
    skip_resolution: bool,
    min_match_score: float | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if skip_resolution:
        return read_jsonl(paths["resolved"]), read_jsonl(paths["unmatched"])

    resolved_rows = read_jsonl(paths["resolved"]) if resume else []
    unmatched_rows = read_jsonl(paths["unmatched"]) if resume else []
    seen_local_ids = {row.get("local_input_id") for row in resolved_rows + unmatched_rows}

    if not resume:
        reset_file(paths["resolved"])
        reset_file(paths["unmatched"])

    for index, local_paper in enumerate(local_papers, 1):
        if local_paper.local_id in seen_local_ids:
            continue
        LOGGER.info("Resolving %d/%d: %s", index, len(local_papers), local_paper.title[:120])
        try:
            result, _ = resolve_paper(
                local_paper,
                client=client,
                config=config.match,
                paper_fields=config.fields.paper_fields,
                search_limit=config.api.search_limit,
                min_match_score=min_match_score,
            )
        except Exception as exc:
            result = failure_result(local_paper, exc)
        row = match_result_to_json(result)
        if row.get("match_status") in ACCEPTED_MATCH_STATUSES and row.get("matched_paper_id"):
            resolved_rows.append(row)
            append_jsonl(paths["resolved"], row)
        else:
            unmatched_rows.append(row)
            append_jsonl(paths["unmatched"], row)

    return resolved_rows, unmatched_rows


def fetch_internal_reference_edges(
    resolved_rows: list[dict[str, Any]],
    matched_paper_id_to_local_id: dict[str, str],
    client: SemanticScholarClient,
    reference_fields: tuple[str, ...],
    references_jsonl_path: Path,
    resume: bool,
) -> list[dict[str, Any]]:
    reference_rows = read_jsonl(references_jsonl_path) if resume else []
    completed_source_ids = {row.get("source_paper_id") for row in reference_rows if row.get("source_paper_id")}
    if not resume:
        reset_file(references_jsonl_path)

    for index, row in enumerate(resolved_rows, 1):
        source_paper_id = row.get("matched_paper_id")
        source_local_id = row.get("local_input_id")
        if not source_paper_id or not source_local_id or source_paper_id in completed_source_ids:
            continue
        LOGGER.info("Fetching references %d/%d: %s", index, len(resolved_rows), source_paper_id)
        try:
            references = client.get_references(source_paper_id, fields=reference_fields)
        except Exception:
            LOGGER.exception("Reference fetch failed for %s", source_paper_id)
            continue

        new_rows = []
        for reference in references:
            cited_paper = reference.get("citedPaper") or {}
            target_paper_id = cited_paper.get("paperId")
            if not target_paper_id:
                continue
            target_local_id = matched_paper_id_to_local_id.get(target_paper_id)
            if target_local_id is None:
                continue
            new_rows.append(
                {
                    "source_local_id": source_local_id,
                    "target_local_id": target_local_id,
                    "source_paper_id": source_paper_id,
                    "target_paper_id": target_paper_id,
                    "source_title": row.get("matched_title") or row.get("local_title") or "",
                    "target_title": cited_paper.get("title") or "",
                    "source_year": row.get("matched_year") or row.get("local_year") or "",
                    "target_year": cited_paper.get("year") or "",
                }
            )
        completed_source_ids.add(source_paper_id)
        for new_row in new_rows:
            reference_rows.append(new_row)
            append_jsonl(references_jsonl_path, new_row)

    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in reference_rows:
        key = (str(row.get("source_local_id")), str(row.get("target_local_id")))
        deduped[key] = row
    return [deduped[key] for key in sorted(deduped)]


def failure_result(local_paper: LocalPaper, exc: Exception) -> Any:
    from matcher import MatchResult

    return MatchResult(
        local_input_id=local_paper.local_id,
        local_title=local_paper.title,
        local_year=local_paper.year,
        matched_paper_id=None,
        matched_title=None,
        matched_year=None,
        match_score=0.0,
        match_status="not_found",
        doi=local_paper.doi,
        dblp_key=local_paper.dblp_key,
        reason=f"resolution_error: {exc}",
    )


def output_path_map(output_dir: Path, config: PipelineConfig) -> dict[str, Path]:
    return {
        "resolved": output_dir / config.paths.resolved_papers_name,
        "unmatched": output_dir / config.paths.unmatched_papers_name,
        "references": output_dir / config.paths.paper_references_name,
        "internal_edges": output_dir / config.paths.internal_edges_name,
        "graphml": output_dir / config.paths.graphml_name,
        "summary": output_dir / config.paths.summary_name,
    }


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def reset_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


if __name__ == "__main__":
    main()
