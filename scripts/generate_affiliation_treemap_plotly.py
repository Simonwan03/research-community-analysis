#!/usr/bin/env python3
"""Generate interactive Plotly treemap HTML files from affiliation treemap CSVs."""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path
import textwrap

import pandas as pd


PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.35.2.min.js"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate interactive Plotly treemap HTML files from the existing "
            "affiliation treemap CSV outputs."
        )
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("data/dblp_ai_authors_2015_2025/results"),
        help="Directory containing affiliation treemap CSV outputs.",
    )
    parser.add_argument(
        "--inputs",
        nargs="*",
        type=Path,
        default=None,
        help=(
            "Specific treemap CSV files to convert. "
            "Defaults to the three standard CSVs in --results-dir."
        ),
    )
    parser.add_argument(
        "--height",
        type=int,
        default=920,
        help="HTML figure height in pixels.",
    )
    parser.add_argument(
        "--top-authors",
        type=int,
        default=12,
        help="Number of top authors to show in the details panel after clicking an affiliation.",
    )
    return parser.parse_args()


def default_input_paths(results_dir: Path) -> list[Path]:
    return [
        results_dir / "affiliation_paper_count_treemap.csv",
        results_dir / "affiliation_paper_count_treemap_fullgraph.csv",
        results_dir / "affiliation_paper_count_treemap_fullgraph_first_author.csv",
    ]


def title_from_stem(stem: str) -> str:
    mapping = {
        "affiliation_paper_count_treemap": "Subgraph · All Authors",
        "affiliation_paper_count_treemap_fullgraph": "Fullgraph · All Authors",
        "affiliation_paper_count_treemap_fullgraph_first_author": "Fullgraph · First Author",
    }
    return mapping.get(stem, stem.replace("_", " ").title())


def author_scope_from_stem(stem: str) -> str:
    return "first" if stem.endswith("_first_author") else "all"


def authors_csv_from_stem(results_dir: Path, stem: str) -> Path:
    data_dir = results_dir.parent
    if stem == "affiliation_paper_count_treemap":
        return data_dir / "authors_orcid_subgraph.csv"
    return data_dir / "authors_orcid_fullgraph.csv"


def papers_csv_from_results_dir(results_dir: Path) -> Path:
    return results_dir.parent / "papers.csv"


def build_palette(names: list[str]) -> dict[str, str]:
    base_colors = [
        "#d24b40",
        "#5388c7",
        "#ff9933",
        "#9a78c9",
        "#61c2d4",
        "#59a541",
        "#9a7a6a",
        "#7cc84e",
        "#d1a34a",
        "#5d9d89",
        "#8479d9",
        "#c65d91",
    ]
    return {
        name: base_colors[index % len(base_colors)]
        for index, name in enumerate(names)
    }


def wrap_label(label: str, width: int) -> str:
    escaped = html.escape(label)
    lines = textwrap.wrap(
        escaped,
        width=max(8, width),
        break_long_words=False,
        break_on_hyphens=False,
    )
    return "<br>".join(lines) if lines else escaped


def make_text_block(label: str, value: int, level: str) -> str:
    label_length = len(label)
    if level == "country":
        label_size = 24
        count_size = 20
        wrap_width = 18
    else:
        if value >= 900:
            label_size, count_size, wrap_width = 23, 21, 20
        elif value >= 500:
            label_size, count_size, wrap_width = 21, 19, 18
        elif value >= 250:
            label_size, count_size, wrap_width = 18, 17, 16
        elif value >= 120:
            label_size, count_size, wrap_width = 16, 15, 14
        elif value >= 60:
            label_size, count_size, wrap_width = 14, 13, 12
        else:
            label_size, count_size, wrap_width = 12, 11, 10

        if label_length > 28:
            label_size -= 1
            wrap_width -= 1
        if label_length > 40:
            label_size -= 2
            wrap_width -= 1

    label_size = max(10, label_size)
    count_size = max(10, count_size)
    wrapped_label = wrap_label(label, wrap_width)
    return (
        f"<span style='font-size:{label_size}px;font-weight:800;"
        "line-height:1.08;text-align:center;display:block'>"
        f"{wrapped_label}</span>"
        f"<span style='font-size:{count_size}px;font-weight:700;"
        "line-height:1.0;display:block;margin-top:4px'>"
        f"{value:,}</span>"
    )


def load_author_metadata(authors_csv: Path) -> dict[str, dict[str, str | int]]:
    authors_df = pd.read_csv(authors_csv, low_memory=False).fillna("")
    metadata: dict[str, dict[str, str | int]] = {}
    for row in authors_df.to_dict(orient="records"):
        author_id = str(row.get("author_id", "")).strip()
        if not author_id:
            continue
        metadata[author_id] = {
            "name": str(row.get("name", "")).strip(),
            "affiliation": str(row.get("affiliation", "")).strip(),
            "orcid": str(row.get("orcid", "")).strip(),
            "paper_count": int(row.get("paper_count", 0) or 0),
            "weighted_degree": int(row.get("weighted_degree", 0) or 0),
        }
    return metadata


def split_pipe(value: object) -> list[str]:
    return [part.strip() for part in str(value or "").split("|") if part.strip()]


def build_top_authors_lookup(
    papers_csv: Path,
    authors_csv: Path,
    author_scope: str,
    top_k: int,
) -> dict[str, list[dict[str, str | int]]]:
    papers_df = pd.read_csv(papers_csv, low_memory=False).fillna("")
    author_metadata = load_author_metadata(authors_csv)

    contribution_counts: dict[tuple[str, str, str], int] = {}
    for row in papers_df.itertuples(index=False):
        author_ids = split_pipe(getattr(row, "author_ids", ""))
        if author_scope == "first":
            author_ids = author_ids[:1]
        for author_id in author_ids:
            metadata = author_metadata.get(author_id)
            if metadata is None:
                continue
            affiliation = str(metadata.get("affiliation", "")).strip()
            if not affiliation:
                continue
            key = (affiliation, author_id, str(metadata.get("name", "")))
            contribution_counts[key] = contribution_counts.get(key, 0) + 1

    grouped: dict[str, list[dict[str, str | int]]] = {}
    for (affiliation, author_id, _), contribution_count in contribution_counts.items():
        metadata = author_metadata[author_id]
        grouped.setdefault(affiliation, []).append(
            {
                "author_id": author_id,
                "name": str(metadata.get("name", "")),
                "orcid": str(metadata.get("orcid", "")),
                "contribution_count": contribution_count,
                "overall_paper_count": int(metadata.get("paper_count", 0)),
                "weighted_degree": int(metadata.get("weighted_degree", 0)),
            }
        )

    for affiliation, rows in grouped.items():
        rows.sort(
            key=lambda item: (
                -int(item["contribution_count"]),
                -int(item["overall_paper_count"]),
                -int(item["weighted_degree"]),
                str(item["name"]),
            )
        )
        grouped[affiliation] = rows[:top_k]
    return grouped


def build_plot_payload(df: pd.DataFrame, title: str) -> dict:
    total_count = int(df["paper_count"].sum())
    country_totals = (
        df.groupby("country_name", as_index=False)["paper_count"]
        .sum()
        .sort_values(by=["paper_count", "country_name"], ascending=[False, True])
    )
    palette = build_palette(country_totals["country_name"].tolist())

    labels: list[str] = ["All Affiliations"]
    parents: list[str] = [""]
    ids: list[str] = ["root"]
    values: list[int] = [total_count]
    colors: list[str] = ["#f4f4f4"]
    texts: list[str] = [""]
    customdata: list[list[str | int | float]] = [
        ["root", "", "", total_count, 100.0, 100.0]
    ]

    for row in country_totals.itertuples(index=False):
        country_name = str(row.country_name)
        country_value = int(row.paper_count)
        country_share = country_value / total_count * 100.0 if total_count else 0.0

        labels.append(country_name)
        parents.append("root")
        ids.append(f"country::{country_name}")
        values.append(country_value)
        colors.append(palette[country_name])
        texts.append(make_text_block(country_name, country_value, level="country"))
        customdata.append(["country", country_name, "", country_value, country_share, 100.0])

        country_affiliations = df.loc[df["country_name"] == country_name].sort_values(
            by=["paper_count", "affiliation"],
            ascending=[False, True],
        )
        for aff_row in country_affiliations.itertuples(index=False):
            affiliation = str(aff_row.affiliation)
            paper_count = int(aff_row.paper_count)
            global_share = float(aff_row.global_share_percent)
            country_share_percent = float(aff_row.country_share_percent)

            labels.append(affiliation)
            parents.append(f"country::{country_name}")
            ids.append(f"aff::{country_name}::{affiliation}")
            values.append(paper_count)
            colors.append(palette[country_name])
            texts.append(make_text_block(affiliation, paper_count, level="affiliation"))
            customdata.append(
                [
                    "affiliation",
                    country_name,
                    affiliation,
                    paper_count,
                    global_share,
                    country_share_percent,
                ]
            )

    data = {
        "type": "treemap",
        "labels": labels,
        "parents": parents,
        "ids": ids,
        "values": values,
        "branchvalues": "total",
        "marker": {
            "colors": colors,
            "line": {"width": 1.5, "color": "white"},
        },
        "text": texts,
        "customdata": customdata,
        "hovertemplate": (
            "<b>%{label}</b><br>"
            "Country: %{customdata[1]}<br>"
            "Paper Count: %{customdata[3]:,}<br>"
            "Global Share: %{customdata[4]:.2f}%<br>"
            "Country Share: %{customdata[5]:.2f}%"
            "<extra></extra>"
        ),
        "texttemplate": "%{text}",
        "textfont": {"size": 14, "color": "white", "family": "Arial, sans-serif"},
        "pathbar": {"visible": True},
        "tiling": {"pad": 2},
        "root": {"color": "#f4f4f4"},
        "maxdepth": 2,
        "textposition": "middle center",
    }

    layout = {
        "title": {
            "text": f"{title}<br><sup>Total Count in Plot: {total_count:,}</sup>",
            "x": 0.5,
            "xanchor": "center",
            "font": {"size": 28},
        },
        "margin": {"t": 88, "l": 10, "r": 10, "b": 10},
        "paper_bgcolor": "white",
        "plot_bgcolor": "white",
        "font": {"family": "Arial, sans-serif", "color": "#111111", "size": 18},
        "uniformtext": {"mode": "hide", "minsize": 8},
    }

    return {"data": [data], "layout": layout}


def render_html(
    payload: dict,
    title: str,
    height: int,
    top_authors_lookup: dict[str, list[dict[str, str | int]]],
    author_scope: str,
) -> str:
    title_escaped = html.escape(title)
    payload_json = json.dumps(payload, ensure_ascii=False)
    top_authors_json = json.dumps(top_authors_lookup, ensure_ascii=False)
    scope_label = "First-author paper count" if author_scope == "first" else "Selected-scope paper count"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title_escaped}</title>
  <script src="{PLOTLY_CDN}"></script>
  <style>
    body {{
      margin: 0;
      font-family: Arial, sans-serif;
      background: linear-gradient(180deg, #fafafa 0%, #f3f4f6 100%);
      color: #111111;
    }}
    .page {{
      max-width: 1800px;
      margin: 0 auto;
      padding: 22px 20px 28px;
    }}
    .hero {{
      text-align: center;
      margin-bottom: 14px;
    }}
    .hero h1 {{
      margin: 0;
      font-size: 34px;
      line-height: 1.15;
      font-weight: 800;
      letter-spacing: -0.02em;
    }}
    .hero p {{
      margin: 8px 0 0;
      font-size: 16px;
      color: #4b5563;
    }}
    .layout {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) 360px;
      gap: 18px;
      align-items: start;
    }}
    .panel {{
      background: rgba(255, 255, 255, 0.92);
      border: 1px solid #e5e7eb;
      border-radius: 18px;
      box-shadow: 0 14px 36px rgba(15, 23, 42, 0.08);
      overflow: hidden;
      min-height: {height}px;
    }}
    .panel-head {{
      padding: 18px 20px 14px;
      border-bottom: 1px solid #eef2f7;
      background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
      text-align: center;
    }}
    .panel-head h2 {{
      margin: 0;
      font-size: 22px;
      font-weight: 800;
    }}
    .panel-head p {{
      margin: 6px 0 0;
      font-size: 14px;
      color: #6b7280;
    }}
    #details {{
      padding: 18px 20px 20px;
      overflow: auto;
      max-height: calc({height}px - 86px);
    }}
    .hint {{
      color: #6b7280;
      font-size: 15px;
      line-height: 1.6;
      text-align: center;
      padding: 28px 12px;
    }}
    .detail-title {{
      margin: 0 0 6px;
      font-size: 24px;
      line-height: 1.2;
      font-weight: 800;
      text-align: center;
    }}
    .detail-sub {{
      margin: 0 0 16px;
      font-size: 15px;
      color: #6b7280;
      text-align: center;
    }}
    .stats {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-bottom: 18px;
    }}
    .stat {{
      background: #f8fafc;
      border: 1px solid #e5e7eb;
      border-radius: 14px;
      padding: 12px 14px;
      text-align: center;
    }}
    .stat .k {{
      display: block;
      font-size: 13px;
      color: #6b7280;
      margin-bottom: 4px;
    }}
    .stat .v {{
      display: block;
      font-size: 20px;
      font-weight: 800;
      color: #111827;
    }}
    .authors-head {{
      font-size: 18px;
      font-weight: 800;
      margin: 14px 0 12px;
      text-align: center;
    }}
    .author-list {{
      list-style: none;
      margin: 0;
      padding: 0;
      display: grid;
      gap: 10px;
    }}
    .author-item {{
      border: 1px solid #e5e7eb;
      border-radius: 14px;
      background: white;
      padding: 12px 14px;
      box-shadow: 0 3px 10px rgba(15, 23, 42, 0.04);
    }}
    .author-top {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: baseline;
      margin-bottom: 6px;
    }}
    .author-name {{
      font-size: 17px;
      font-weight: 800;
      color: #111827;
    }}
    .author-rank {{
      font-size: 13px;
      color: #6b7280;
      font-weight: 700;
    }}
    .author-meta {{
      display: grid;
      grid-template-columns: 1fr 1fr 1fr;
      gap: 8px;
      font-size: 13px;
      color: #4b5563;
    }}
    .author-meta span {{
      display: block;
      text-align: center;
      background: #f8fafc;
      border-radius: 10px;
      padding: 7px 8px;
    }}
    .chart-card {{
      background: rgba(255, 255, 255, 0.94);
      border: 1px solid #e5e7eb;
      border-radius: 22px;
      box-shadow: 0 18px 44px rgba(15, 23, 42, 0.08);
      overflow: hidden;
      padding: 12px 12px 6px;
    }}
    #chart {{
      width: 100%;
      height: {height}px;
    }}
    @media (max-width: 1200px) {{
      .layout {{
        grid-template-columns: 1fr;
      }}
      .panel {{
        min-height: 0;
      }}
      #details {{
        max-height: none;
      }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <div class="hero">
      <h1>{title_escaped}</h1>
      <p>Interactive affiliation treemap. Click an affiliation block to inspect its top authors.</p>
    </div>
    <div class="layout">
      <div class="chart-card">
        <div id="chart"></div>
      </div>
      <aside class="panel">
        <div class="panel-head">
          <h2>Affiliation Details</h2>
          <p>Click one affiliation block in the treemap.</p>
        </div>
        <div id="details">
          <div class="hint">
            Select an affiliation to see its paper count, country share, and top authors.
          </div>
        </div>
      </aside>
    </div>
  </div>
  <script>
    const figure = {payload_json};
    const topAuthorsLookup = {top_authors_json};
    const scopeLabel = {json.dumps(scope_label)};
    Plotly.newPlot('chart', figure.data, figure.layout, {{
      responsive: true,
      displaylogo: false,
      toImageButtonOptions: {{
        format: 'png',
        filename: '{html.escape(title).replace(" ", "_").lower()}',
        scale: 2
      }}
    }});

    const chart = document.getElementById('chart');
    const details = document.getElementById('details');

    function formatNumber(value) {{
      return new Intl.NumberFormat('en-US').format(value);
    }}

    function renderDefault() {{
      details.innerHTML = `
        <div class="hint">
          Select an affiliation to see its paper count, country share, and top authors.
        </div>
      `;
    }}

    function renderAffiliation(point) {{
      const kind = point.customdata[0];
      if (kind !== 'affiliation') {{
        renderDefault();
        return;
      }}

      const country = point.customdata[1];
      const affiliation = point.customdata[2];
      const paperCount = point.customdata[3];
      const globalShare = point.customdata[4];
      const countryShare = point.customdata[5];
      const authors = topAuthorsLookup[affiliation] || [];

      const authorItems = authors.length
        ? authors.map((author, index) => `
            <li class="author-item">
              <div class="author-top">
                <span class="author-name">${{author.name || 'Unknown Author'}}</span>
                <span class="author-rank">#${{index + 1}}</span>
              </div>
              <div class="author-meta">
                <span><strong>${{formatNumber(author.contribution_count)}}</strong><br>${{scopeLabel}}</span>
                <span><strong>${{formatNumber(author.overall_paper_count)}}</strong><br>Overall papers</span>
                <span><strong>${{formatNumber(author.weighted_degree)}}</strong><br>Weighted degree</span>
              </div>
            </li>
          `).join('')
        : `<div class="hint">No author details were available for this affiliation.</div>`;

      details.innerHTML = `
        <h3 class="detail-title">${{affiliation}}</h3>
        <p class="detail-sub">${{country}}</p>
        <div class="stats">
          <div class="stat">
            <span class="k">Paper Count</span>
            <span class="v">${{formatNumber(paperCount)}}</span>
          </div>
          <div class="stat">
            <span class="k">Global Share</span>
            <span class="v">${{globalShare.toFixed(2)}}%</span>
          </div>
          <div class="stat">
            <span class="k">Country Share</span>
            <span class="v">${{countryShare.toFixed(2)}}%</span>
          </div>
          <div class="stat">
            <span class="k">Top Authors Shown</span>
            <span class="v">${{formatNumber(authors.length)}}</span>
          </div>
        </div>
        <div class="authors-head">Top Authors</div>
        <ul class="author-list">${{authorItems}}</ul>
      `;
    }}

    chart.on('plotly_click', function(event) {{
      if (!event || !event.points || !event.points.length) {{
        renderDefault();
        return;
      }}
      renderAffiliation(event.points[0]);
    }});

    renderDefault();
  </script>
</body>
</html>
"""


def convert_one(csv_path: Path, height: int, top_k: int) -> Path:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CSV: {csv_path}")
    df = pd.read_csv(csv_path, low_memory=False)
    title = title_from_stem(csv_path.stem)
    author_scope = author_scope_from_stem(csv_path.stem)
    authors_csv = authors_csv_from_stem(csv_path.parent, csv_path.stem)
    papers_csv = papers_csv_from_results_dir(csv_path.parent)
    payload = build_plot_payload(df, title)
    top_authors_lookup = build_top_authors_lookup(
        papers_csv=papers_csv,
        authors_csv=authors_csv,
        author_scope=author_scope,
        top_k=top_k,
    )
    html_text = render_html(
        payload,
        title,
        height=height,
        top_authors_lookup=top_authors_lookup,
        author_scope=author_scope,
    )
    output_path = csv_path.with_suffix(".html")
    output_path.write_text(html_text, encoding="utf-8")
    return output_path


def main() -> None:
    args = parse_args()
    input_paths = args.inputs or default_input_paths(args.results_dir)

    for csv_path in input_paths:
        output_path = convert_one(csv_path, height=args.height, top_k=args.top_authors)
        print(f"Saved Plotly treemap HTML to {output_path}")


if __name__ == "__main__":
    main()
