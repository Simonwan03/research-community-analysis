#!/usr/bin/env python3
"""Build an interactive ipysigma view for the coauthor graph."""

from __future__ import annotations

import argparse
from pathlib import Path

from enrich_top_authors_profiles import (
    clean_author_name as clean_query_name,
    lookup_openalex_author,
    lookup_wikidata_citizenship,
    resolve_author_orcid,
)
from visualize_coauthor_graph import (
    build_graph,
    build_community_color_map,
    choose_label_nodes,
    clean_author_name,
    compute_bridge_scores,
    detect_communities,
    filter_graph_by_edge_weight,
    filter_small_communities,
    load_precomputed_community_membership,
    save_community_assignments,
    select_subgraph,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render the current ORCID + affiliation coauthor subgraph as an interactive ipysigma HTML view."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025"),
        help="Directory containing authors_orcid_subgraph.csv and edges_orcid_subgraph.csv.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output HTML path. Defaults to <input-dir>/orcid_subgraph_top120_sigma.html.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=120,
        help="Keep the top-k authors by weighted degree for visualization.",
    )
    parser.add_argument(
        "--label-top-k",
        type=int,
        default=10,
        help="Always label the global top-k authors, plus each community's top author.",
    )
    parser.add_argument(
        "--min-edge-weight",
        type=int,
        default=3,
        help="Filter out edges lighter than this weight before visualization.",
    )
    parser.add_argument(
        "--height",
        type=int,
        default=820,
        help="HTML view height in pixels.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible metric selection.",
    )
    parser.add_argument(
        "--plot-full-graph",
        action="store_true",
        help="Plot the full filtered ORCID subgraph instead of selecting a top-k display subgraph.",
    )
    parser.add_argument(
        "--min-community-count",
        type=int,
        default=1,
        help="Drop communities smaller than this size before visualization. Default: 1",
    )
    parser.add_argument(
        "--enrich-core-authors",
        action="store_true",
        help="Fetch employer and citizenship for labeled core authors and add them to node attributes.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.5,
        help="Delay between external enrichment API requests.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=60.0,
        help="HTTP timeout for each enrichment request.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Maximum retries for transient enrichment network failures.",
    )
    return parser.parse_args()


def enrich_core_author_nodes(
    subgraph,
    label_nodes,
    sleep_seconds: float,
    timeout_seconds: float,
    max_retries: int,
):
    import time

    for node in label_nodes:
        raw_name = subgraph.nodes[node].get("name", node)
        query_name = clean_query_name(raw_name)
        dblp_pid = subgraph.nodes[node].get("dblp_pid", "")
        paper_orcid = subgraph.nodes[node].get("orcid", "")
        orcid_fields = resolve_author_orcid(
            dblp_pid,
            paper_orcid,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        resolved_orcid = orcid_fields.get("resolved_orcid", "")
        print(
            f"[enrich] {raw_name} -> query={query_name} | "
            f"resolved_orcid={resolved_orcid or '(none)'} | "
            f"orcid_source={orcid_fields.get('orcid_source', '')} | "
            f"orcid_status={orcid_fields.get('orcid_status', '')}",
            flush=True,
        )
        try:
            openalex_fields = lookup_openalex_author(
                resolved_orcid,
                query_name,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
            )
            print(
                "  "
                f"openalex_status={openalex_fields.get('openalex_match_status', '')} | "
                f"query_type={openalex_fields.get('openalex_query_type', '')} | "
                f"employer_status={openalex_fields.get('employer_status', '')} | "
                f"employer_source={openalex_fields.get('employer_source', '')} | "
                f"employer={openalex_fields.get('employer', '') or '(none)'}",
                flush=True,
            )
        except Exception as exc:
            openalex_fields = {
                "employer": "",
            }
            print(f"  openalex_error={type(exc).__name__}: {exc}", flush=True)
        time.sleep(sleep_seconds)

        try:
            wikidata_fields = lookup_wikidata_citizenship(
                query_name,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
            )
            print(
                "  "
                f"wikidata_status={wikidata_fields.get('wikidata_match_status', '')} | "
                f"citizenship={wikidata_fields.get('country_of_citizenship', '') or '(none)'}",
                flush=True,
            )
        except Exception as exc:
            wikidata_fields = {
                "country_of_citizenship": "",
            }
            print(f"  wikidata_error={type(exc).__name__}: {exc}", flush=True)
        time.sleep(sleep_seconds)

        subgraph.nodes[node]["employer"] = openalex_fields.get("employer", "")
        subgraph.nodes[node]["citizenship"] = wikidata_fields.get("country_of_citizenship", "")
        subgraph.nodes[node]["orcid"] = resolved_orcid


def annotate_graph(
    subgraph,
    full_community_membership,
    weighted_degree,
    full_bridge_scores,
    label_top_k,
    plot_full_graph: bool = False,
    enrich_core_authors: bool = False,
    sleep_seconds: float = 0.5,
    timeout_seconds: float = 60.0,
    max_retries: int = 5,
):
    community_membership = {
        node: full_community_membership[node]
        for node in subgraph.nodes()
    }
    bridge_scores = {
        node: full_bridge_scores.get(node, 0.0)
        for node in subgraph.nodes()
    }
    community_color_map = build_community_color_map(list(community_membership.values()))
    label_nodes = set(
        choose_label_nodes(
            subgraph,
            community_membership,
            weighted_degree,
            label_top_k=label_top_k,
            include_community_representatives=not plot_full_graph,
        )
    )
    print(
        f"[annotate] enrich_core_authors={enrich_core_authors} | labeled_core_authors={len(label_nodes)}",
        flush=True,
    )

    for node in subgraph.nodes():
        clean_name = clean_author_name(subgraph.nodes[node].get("name", node))
        subgraph.nodes[node]["label"] = clean_name
        subgraph.nodes[node]["label_size"] = 14 if node in label_nodes else 0
        subgraph.nodes[node]["pid"] = subgraph.nodes[node].get("dblp_pid", "")
        subgraph.nodes[node]["orcid"] = subgraph.nodes[node].get("orcid", "")
        subgraph.nodes[node]["affiliation"] = subgraph.nodes[node].get("affiliation", "")
        subgraph.nodes[node]["all_affiliations"] = subgraph.nodes[node].get("all_affiliations", "")
        subgraph.nodes[node]["employer"] = ""
        subgraph.nodes[node]["citizenship"] = ""
        subgraph.nodes[node]["community"] = f"Community {community_membership[node] + 1}"
        subgraph.nodes[node]["community_color"] = community_color_map[community_membership[node]]
        subgraph.nodes[node]["weighted_degree"] = float(weighted_degree.get(node, 0.0))
        subgraph.nodes[node]["bridge_score"] = float(bridge_scores.get(node, 0.0))
        subgraph.nodes[node]["is_bridge"] = bridge_scores.get(node, 0.0) > 0
        subgraph.nodes[node]["border_size"] = 2 if subgraph.nodes[node]["is_bridge"] else 0
        subgraph.nodes[node]["border_color"] = "#111827"

    if enrich_core_authors and label_nodes:
        print("[annotate] starting core author enrichment", flush=True)
        enrich_core_author_nodes(
            subgraph,
            sorted(label_nodes),
            sleep_seconds=sleep_seconds,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
    elif enrich_core_authors:
        print("[annotate] enrichment requested but no labeled core authors were selected", flush=True)
    else:
        print("[annotate] enrichment disabled; skipping employer/citizenship lookup", flush=True)

    for left, right, data in subgraph.edges(data=True):
        left_community = community_membership[left]
        right_community = community_membership[right]
        is_cross = left_community != right_community
        data["edge_kind"] = "Cross-community" if is_cross else "Within-community"
        data["edge_color"] = "#334155" if is_cross else "#cbd5e1"
        data["edge_weight"] = float(data.get("weight", 1))

    return subgraph


def main() -> None:
    try:
        from ipysigma import Sigma
    except ImportError as exc:
        raise SystemExit(
            "ipysigma is not installed. Install it with: pip install ipysigma"
        ) from exc

    args = parse_args()
    output_path = args.output or (
        args.input_dir / (
            "orcid_subgraph_full_sigma.html"
            if args.plot_full_graph
            else f"orcid_subgraph_top{args.top_k}_sigma.html"
        )
    )
    community_output_path = args.input_dir / "community_assignments_orcid_subgraph_visualization.csv"

    graph, authors_df = build_graph(args.input_dir)
    filtered_graph = filter_graph_by_edge_weight(graph, args.min_edge_weight)
    precomputed_membership = load_precomputed_community_membership(args.input_dir)
    if precomputed_membership and all(node in precomputed_membership for node in filtered_graph.nodes()):
        full_community_membership = {
            node: precomputed_membership[node]
            for node in filtered_graph.nodes()
        }
    else:
        full_community_membership = detect_communities(filtered_graph)
    filtered_graph, full_community_membership = filter_small_communities(
        filtered_graph,
        full_community_membership,
        min_community_count=args.min_community_count,
    )
    subgraph, filtered_graph, weighted_degree = select_subgraph(
        filtered_graph,
        authors_df,
        top_k=args.top_k,
        plot_full_graph=args.plot_full_graph,
    )
    full_bridge_scores = compute_bridge_scores(filtered_graph, full_community_membership)
    save_community_assignments(
        community_output_path,
        filtered_graph,
        subgraph,
        full_community_membership,
        weighted_degree,
        full_bridge_scores,
    )
    annotate_graph(
        subgraph,
        full_community_membership,
        weighted_degree,
        full_bridge_scores,
        label_top_k=args.label_top_k,
        plot_full_graph=args.plot_full_graph,
        enrich_core_authors=args.enrich_core_authors,
        sleep_seconds=args.sleep_seconds,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
    )

    Sigma.write_html(
        subgraph,
        output_path,
        height=args.height,
        background_color="white",
        start_layout=8,
        node_size="weighted_degree",
        raw_node_color="community_color",
        raw_node_label="label",
        raw_node_label_size="label_size",
        default_node_label="",
        raw_node_border_color="border_color",
        raw_node_border_size="border_size",
        raw_edge_size="edge_weight",
        raw_edge_color="edge_color",
        default_edge_type="line",
        clickable_edges=True,
        hide_info_panel=False,
        hide_search=False,
    )

    print(
        f"Saved interactive ipysigma view to {output_path} "
        f"with {subgraph.number_of_nodes()} nodes and {subgraph.number_of_edges()} edges."
    )


if __name__ == "__main__":
    main()
