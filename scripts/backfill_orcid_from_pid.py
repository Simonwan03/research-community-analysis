#!/usr/bin/env python3
"""Backfill missing ORCID values in a CSV from a local DBLP dump."""

from __future__ import annotations

import argparse
import gzip
import io
import re
from pathlib import Path

import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill missing ORCID values in a CSV using a local DBLP dump."
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025/authors_filtered_full_graph.csv"),
        help="CSV file containing at least dblp_pid and orcid columns.",
    )
    parser.add_argument(
        "--dblp-xml-gz",
        type=Path,
        default=Path("dblp_data/dblp.xml.gz"),
        help="Local DBLP full dump (.xml.gz). When present, pid->ORCID is resolved from this file first.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=None,
        help="Output CSV path. Defaults to <input stem>_orcid_backfilled.csv.",
    )
    return parser.parse_args()


def normalize_orcid(orcid: str | None) -> str:
    value = (orcid or "").strip()
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


def lookup_orcids_from_local_dump(
    dblp_xml_gz: Path,
    target_pids: set[str],
) -> dict[str, str]:
    results: dict[str, str] = {}
    if not dblp_xml_gz.exists() or not target_pids:
        return results

    homepages_start_pattern = re.compile(
        r'<www\b[^>]*\bkey="homepages/([^"]+)"[^>]*>',
        re.IGNORECASE,
    )
    url_orcid_pattern = re.compile(
        r"<url>\s*https?://orcid\.org/([0-9X-]{19})\s*</url>",
        re.IGNORECASE,
    )
    progress = None

    try:
        inside_www = False
        current_pid = ""
        current_lines: list[str] = []
        total_bytes = dblp_xml_gz.stat().st_size
        with dblp_xml_gz.open("rb") as raw_handle:
            if tqdm is not None:
                progress = tqdm(
                    total=total_bytes,
                    desc="Scan local DBLP dump",
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                )
            with gzip.GzipFile(fileobj=raw_handle, mode="rb") as gz_handle:
                with io.TextIOWrapper(gz_handle, encoding="ISO-8859-1", errors="replace") as handle:
                    last_pos = raw_handle.tell()
                    for line in handle:
                        current_pos = raw_handle.tell()
                        if progress is not None and current_pos > last_pos:
                            progress.update(current_pos - last_pos)
                            last_pos = current_pos

                        if not inside_www:
                            match = homepages_start_pattern.search(line)
                            if not match:
                                continue
                            inside_www = True
                            current_pid = match.group(1).strip()
                            current_lines = [line]
                            if "</www>" in line:
                                inside_www = False
                                block = "".join(current_lines)
                                orcid_match = url_orcid_pattern.search(block)
                                if current_pid in target_pids and current_pid not in results:
                                    results[current_pid] = (
                                        normalize_orcid(orcid_match.group(1)) if orcid_match else ""
                                    )
                                    if len(results) == len(target_pids):
                                        break
                            continue

                        current_lines.append(line)
                        if "</www>" not in line:
                            continue

                        inside_www = False
                        block = "".join(current_lines)
                        orcid_match = url_orcid_pattern.search(block)
                        if current_pid in target_pids and current_pid not in results:
                            results[current_pid] = (
                                normalize_orcid(orcid_match.group(1)) if orcid_match else ""
                            )
                            if len(results) == len(target_pids):
                                break

                    if progress is not None and progress.n < total_bytes:
                        progress.update(total_bytes - progress.n)
    finally:
        if progress is not None:
            progress.close()

    return results


def main() -> None:
    args = parse_args()
    output_csv = args.output_csv or args.input_csv.with_name(
        f"{args.input_csv.stem}_orcid_backfilled.csv"
    )

    df = pd.read_csv(args.input_csv)
    if "dblp_pid" not in df.columns or "orcid" not in df.columns:
        raise SystemExit("Input CSV must contain dblp_pid and orcid columns.")

    df["orcid"] = df["orcid"].fillna("").astype(str).str.strip()
    df["dblp_pid"] = df["dblp_pid"].fillna("").astype(str).str.strip()

    all_with_pid_mask = df["dblp_pid"] != ""
    initial_missing_count = int(((df["orcid"] == "") & all_with_pid_mask).sum())
    cache: dict[str, str] = {}
    target_pids = set(df.loc[all_with_pid_mask, "dblp_pid"].tolist())
    if args.dblp_xml_gz.exists():
        print(f"Scanning local DBLP dump: {args.dblp_xml_gz}")
        cache.update(lookup_orcids_from_local_dump(args.dblp_xml_gz, target_pids))
        local_hits = sum(1 for pid in target_pids if cache.get(pid, ""))
        local_scanned = len(target_pids)
        local_misses = local_scanned - local_hits
        local_hit_rate = (local_hits / local_scanned * 100.0) if local_scanned else 0.0
        print(
            "[local-dblp] "
            f"target_pids={local_scanned} | "
            f"hits={local_hits} | "
            f"misses={local_misses} | "
            f"hit_rate={local_hit_rate:.2f}%"
        )
    else:
        print(f"Local DBLP dump not found at {args.dblp_xml_gz}, falling back to network lookups.")

    # Apply local dump results to every author row with a pid, not only the rows that were
    # originally missing ORCID. This keeps the CSV aligned with the local DBLP snapshot.
    for index in df.index[df["dblp_pid"] != ""].tolist():
        dblp_pid = df.at[index, "dblp_pid"]
        if cache.get(dblp_pid, ""):
            df.at[index, "orcid"] = cache[dblp_pid]

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False, encoding="utf-8")

    remaining_missing = int((df["orcid"] == "").sum())
    filled = initial_missing_count - remaining_missing
    print(f"Saved backfilled CSV to {output_csv}")
    print(f"[orcid] filled={filled} | remaining_missing={remaining_missing}")


if __name__ == "__main__":
    main()
