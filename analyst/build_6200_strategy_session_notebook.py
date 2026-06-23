"""Build a minimal report notebook for the rebuilt long-only D10 strategy."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import date
from pathlib import Path
from textwrap import dedent

import nbformat as nbf


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SESSION_ID = "strat_solusdt_fw60_combo_2101_2605"
REPORT_SUFFIX = "backtest_analysis"


def default_title(session_id: str) -> str:
    return f"{session_id} - Long D10 Strategy Report"


def default_subtitle(session_id: str) -> str:
    return f"{session_id} | long-only D10 median-TP backtest"


def report_stem(session_id: str) -> str:
    return f"{session_id}_{REPORT_SUFFIX}"


def default_out_notebook(session_id: str) -> Path:
    return ROOT / "artifacts" / session_id / f"{report_stem(session_id)}.ipynb"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a minimal strategy report notebook.")
    parser.add_argument("--session-id", default=DEFAULT_SESSION_ID)
    parser.add_argument("--out")
    parser.add_argument("--title")
    parser.add_argument("--subtitle")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--render-html", action="store_true")
    args = parser.parse_args()
    if not args.out:
        args.out = str(default_out_notebook(args.session_id))
    if not args.title:
        args.title = default_title(args.session_id)
    if not args.subtitle:
        args.subtitle = default_subtitle(args.session_id)
    return args


def raw_cell(source: str):
    return nbf.v4.new_raw_cell(source=source.splitlines(keepends=True))


def md_cell(source: str):
    return nbf.v4.new_markdown_cell(source=(dedent(source).strip() + "\n").splitlines(keepends=True))


def code_cell(source: str):
    return nbf.v4.new_code_cell(source=(dedent(source).strip() + "\n").splitlines(keepends=True))


def frontmatter(*, title: str, subtitle: str, report_date: str, css_path: str) -> str:
    return dedent(
        f"""
        ---
        title: "{title}"
        subtitle: "{subtitle}"
        date: "{report_date}"
        format:
          html:
            theme: cosmo
            css: {css_path}
            toc: true
            toc-title: "Tartalom"
            toc-location: left
            toc-depth: 2
            number-sections: true
            page-layout: article
            code-fold: true
            code-tools: true
            code-copy: true
            df-print: paged
            embed-resources: true
            self-contained: true
        execute:
          enabled: true
          echo: true
          warning: false
          message: false
          freeze: false
        ---
        """
    ).strip()


def build_notebook(
    *,
    session_id: str,
    title: str,
    subtitle: str,
    report_date: str,
    css_path: str,
) -> nbf.NotebookNode:
    nb = nbf.v4.new_notebook()
    nb.cells = [
        raw_cell(frontmatter(title=title, subtitle=subtitle, report_date=report_date, css_path=css_path)),
        code_cell(
            f"""
            #| label: setup
            #| include: false

            from pathlib import Path
            import sys
            import json
            import pandas as pd

            def find_repo_root(start: Path) -> Path:
                cur = start.resolve()
                for candidate in [cur, *cur.parents]:
                    if (candidate / "analyst").exists() and (candidate / "config").exists():
                        return candidate
                raise RuntimeError("Repo root not found from notebook working directory.")

            _root = find_repo_root(Path.cwd())
            if str(_root) not in sys.path:
                sys.path.insert(0, str(_root))

            from analyst.table_formatting import display_analysis_table

            SESSION_ID = "{session_id}"
            artifact_path = _root / "artifacts" / SESSION_ID / "strategy_artifact.json"
            artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
            trades_df = pd.read_csv(_root / "artifacts" / SESSION_ID / artifact["trade_csv_path"], parse_dates=["entry_time", "exit_time"])
            summary_df = pd.read_csv(_root / "artifacts" / SESSION_ID / artifact["summary_csv_path"])

            trade_report_df = trades_df.loc[:, [
                "entry_time",
                "exit_time",
                "entry_price",
                "exit_price",
                "profit_pct",
                "expected_log_return",
                "fact_log_return",
                "fact_1h_max_range_log_return",
                "exit_reason",
            ]].copy()
            """
        ),
        md_cell(
            """
            ## Logika

            Ez a riport a jelenlegi baseline szabályt mutatja:
            csak `long`, csak `D10`, belépés `score_pct_long >= 0.90`,
            take profit a D10 medián 1 órás target log returnjén,
            stop loss ennek szimmetrikus negatívja,
            különben zárás 60 perc után az aktuális close áron.
            TP vagy SL utáni korai zárás után a következő bartól újra beléphet.
            """
        ),
        code_cell(
            """
            #| label: tbl-trade-ledger
            #| tbl-cap: "Trade ledger: entry, exit, várakozás és tény"

            display_analysis_table(trade_report_df)
            """
        ),
        code_cell(
            """
            #| label: tbl-summary
            #| tbl-cap: "Összefoglaló exit reason szerint"

            display_analysis_table(summary_df)
            """
        ),
    ]

    nb.metadata = {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "pygments_lexer": "ipython3",
        },
    }
    return nb


def render_html(out_notebook: Path) -> Path:
    subprocess.run(
        ["quarto", "render", str(out_notebook), "--execute"],
        check=True,
        cwd=ROOT,
    )
    return out_notebook.with_suffix(".html")


def main() -> None:
    args = parse_args()
    out_notebook = Path(args.out).resolve()
    css_path = Path(os.path.relpath(ROOT / "analyst" / "chronoquant_analysis.css", out_notebook.parent))
    out_notebook.parent.mkdir(parents=True, exist_ok=True)
    nb = build_notebook(
        session_id=args.session_id,
        title=args.title,
        subtitle=args.subtitle,
        report_date=args.date,
        css_path=css_path.as_posix(),
    )
    out_notebook.write_text(json.dumps(nb, indent=2, ensure_ascii=False), encoding="utf-8")
    print(out_notebook)
    if args.render_html:
        out_html = render_html(out_notebook)
        print(out_html)


if __name__ == "__main__":
    main()
