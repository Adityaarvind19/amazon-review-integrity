"""Static, self-contained HTML dashboard (+ PNG figures for the README) built from gold marts."""
from __future__ import annotations

import base64
import html
import io
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from .analytics import connect  # noqa: E402

BLUE, ORANGE, INK, INK2, GRID, SURF = "#2a78d6", "#eb6834", "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def _style(ax, title, ylabel=None):
    ax.set_title(title, loc="left", fontsize=11, color=INK, pad=10)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=8)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    if ylabel:
        ax.set_ylabel(ylabel, color=INK2, fontsize=8)


def _fig_to(fig, png_path: Path) -> str:
    fig.patch.set_facecolor(SURF)
    fig.tight_layout()
    fig.savefig(png_path, dpi=150, facecolor=SURF)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, facecolor=SURF)
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def _table(df, max_rows=15) -> str:
    head = "".join(f"<th>{html.escape(str(c))}</th>" for c in df.columns)
    body = "".join("<tr>" + "".join(f"<td>{html.escape('' if v is None else str(v))}</td>" for v in r) + "</tr>"
                   for r in df.head(max_rows).itertuples(index=False))
    return f"<div class='tw'><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"


def build(cfg: dict) -> str:
    res = Path(cfg["paths"]["results_dir"])
    figs = res / "figures"
    figs.mkdir(parents=True, exist_ok=True)
    con = connect(cfg)
    q = lambda s: con.execute(s).df()  # noqa: E731
    k = q("""SELECT count(*) reviews, count(DISTINCT product_key) products, count(DISTINCT reviewer_key) reviewers,
                    avg(is_flagged::INT) flagged_share, avg(is_incentivized::INT) incent_share FROM fact_review""").iloc[0]
    grades = q("SELECT integrity_grade g, count(*) n FROM mart_product_integrity WHERE n_reviews >= 10 GROUP BY 1 ORDER BY 1")
    camp = q("SELECT campaign_type, count(*) n FROM mart_campaigns GROUP BY 1 ORDER BY 2 DESC") \
        if "mart_campaigns" in {r[0] for r in con.execute("SHOW TABLES").fetchall()} else None
    yearly = q("""SELECT d.year, avg(f.is_incentivized::INT)*100 incent_pct, avg(f.is_flagged::INT)*100 flagged_pct,
                         avg(f.rating) raw_avg, avg(CASE WHEN NOT f.is_flagged THEN f.rating END) trusted_avg, count(*) n
                  FROM fact_review f JOIN dim_date d USING(date_key) GROUP BY 1 ORDER BY 1""")
    yearly = yearly[yearly.n >= 50]
    inflated = q("""SELECT parent_asin, left(product_title,40) title, n_reviews, n_flagged,
                           round(raw_bayes_rating,2) raw, round(trust_adjusted_rating,2) trusted,
                           round(rating_inflation,2) inflation, integrity_score score
                    FROM mart_product_integrity WHERE n_reviews >= 20 ORDER BY rating_inflation DESC LIMIT 15""")
    reasons = q("""SELECT code, count(*) n FROM (SELECT unnest(reason_codes) code FROM fact_review WHERE is_flagged)
                   GROUP BY 1 ORDER BY 2 DESC""")
    campaigns = q("""SELECT campaign_type, subtype, n_reviews, n_reviewers, n_products, round(avg_rating,2) avg_rating,
                            round(verified_share,2) verified, first_ts::DATE first_day, example_parent_asin
                     FROM mart_campaigns ORDER BY n_reviews DESC LIMIT 15""") if camp is not None else None

    imgs = []
    fig, ax = plt.subplots(figsize=(5.2, 3.0))
    ax.bar(grades.g, grades.n, color=BLUE, width=0.6)
    for x, y in zip(grades.g, grades.n):
        ax.annotate(f"{y:,}", (x, y), ha="center", va="bottom", fontsize=8, color=INK2, xytext=(0, 2), textcoords="offset points")
    _style(ax, "Products by integrity grade (≥10 reviews)", "products")
    imgs.append(("Integrity grades", _fig_to(fig, figs / "integrity_grades.png")))

    fig, ax = plt.subplots(figsize=(5.2, 3.0))
    ax.bar(yearly.year.astype(str), yearly.incent_pct, color=BLUE, width=0.6)
    if (yearly.year == 2016).any():
        i = list(yearly.year).index(2016)
        ax.axvline(i + 0.75, color=INK2, linestyle="--", linewidth=1)
        ax.annotate("Oct 2016 ban", (i + 0.8, ax.get_ylim()[1] * 0.9), fontsize=8, color=INK2)
    ax.tick_params(axis="x", rotation=45)
    _style(ax, "Reviews disclosing an incentive, by year", "% of reviews")
    imgs.append(("Incentivized disclosures", _fig_to(fig, figs / "incentivized_by_year.png")))

    fig, ax = plt.subplots(figsize=(5.2, 3.0))
    ax.plot(yearly.year, yearly.raw_avg, color=ORANGE, linewidth=2, marker="o", markersize=4, label="raw average")
    ax.plot(yearly.year, yearly.trusted_avg, color=BLUE, linewidth=2, marker="o", markersize=4, label="trusted (flagged removed)")
    ax.legend(frameon=False, fontsize=8, labelcolor=INK2)
    _style(ax, "Average star rating: raw vs trust-adjusted", "stars")
    imgs.append(("Raw vs trusted", _fig_to(fig, figs / "raw_vs_trusted_by_year.png")))

    eval_path = res / "eval" / "eval_report.json"
    eval_html = "<p class='muted'>No evaluation report yet: run <code>python -m revint eval</code>.</p>"
    if eval_path.exists():
        ev = json.loads(eval_path.read_text())["aggregate"]
        types = [t for t, v in ev["recall_by_campaign_type"].items() if v]
        vals = [ev["recall_by_campaign_type"][t]["mean"] * 100 for t in types]
        errs = [ev["recall_by_campaign_type"][t]["std"] * 100 for t in types]
        fig, ax = plt.subplots(figsize=(5.2, 3.0))
        ax.bar(types, vals, yerr=errs, color=BLUE, width=0.6, ecolor=INK2, capsize=3)
        for x, y in zip(types, vals):
            ax.annotate(f"{y:.0f}%", (x, y), ha="center", va="bottom", fontsize=8, color=INK2, xytext=(0, 3), textcoords="offset points")
        ax.set_ylim(0, 110)
        ax.tick_params(axis="x", rotation=20)
        _style(ax, "Recall on injected campaigns, by type", "% of fake reviews flagged")
        imgs.append(("Evaluation recall", _fig_to(fig, figs / "eval_recall_by_type.png")))
        f = lambda m: "n/a" if not ev.get(m) else f"{ev[m]['mean']*100:.1f}%"  # noqa: E731
        auc = ev.get("product_auc") or {}
        auc_s = f"{auc['mean']:.3f}" if auc else "n/a"
        eval_html = (f"<div class='kpis'>{_kpi('Review recall', f('recall_combined'))}"
                     f"{_kpi('Precision on targeted products', f('precision_on_target_products'))}"
                     f"{_kpi('Organic flag rate', f('organic_flag_rate'))}"
                     f"{_kpi('Product AUC', auc_s)}"
                     f"{_kpi('Rating distortion removed', f('rating_distortion_removed'))}</div>")

    kpis = "".join([_kpi("Reviews", f"{int(k.reviews):,}"), _kpi("Products", f"{int(k.products):,}"),
                    _kpi("Reviewers", f"{int(k.reviewers):,}"), _kpi("Flagged reviews", f"{k.flagged_share*100:.2f}%"),
                    _kpi("Incentivized disclosures", f"{k.incent_share*100:.2f}%")])
    camp_kpis = "".join(_kpi(r.campaign_type.replace("_", " ").title(), f"{int(r.n):,}") for r in camp.itertuples()) \
        if camp is not None else ""
    charts = "".join(f"<figure><img alt='{html.escape(t)}' src='data:image/png;base64,{b}'/></figure>" for t, b in imgs)
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Review Integrity Dashboard</title><style>
:root{{--bg:#f9f9f7;--surf:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--line:#e4e3df}}
body{{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 -apple-system,Segoe UI,Roboto,sans-serif}}
main{{max-width:1180px;margin:0 auto;padding:24px 16px}} h1{{font-size:22px;margin:0 0 4px}} h2{{font-size:16px;margin:28px 0 10px}}
.muted{{color:var(--ink2)}} .kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:10px}}
.kpi{{background:var(--surf);border:1px solid var(--line);border-radius:8px;padding:12px}} .kpi b{{display:block;font-size:22px}}
.kpi span{{color:var(--ink2);font-size:12px}} .charts{{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:12px}}
figure{{margin:0;background:var(--surf);border:1px solid var(--line);border-radius:8px;padding:8px}} img{{width:100%;height:auto}}
.tw{{overflow-x:auto;background:var(--surf);border:1px solid var(--line);border-radius:8px}} table{{border-collapse:collapse;width:100%;font-size:12.5px}}
th,td{{text-align:left;padding:6px 10px;border-bottom:1px solid var(--line);white-space:nowrap}} th{{color:var(--ink2);font-weight:600}}
</style></head><body><main>
<h1>Amazon Review Integrity</h1><p class="muted">Generated from the gold layer (DuckDB over Parquet). Categories: {html.escape(', '.join(cfg['pipeline']['categories']))}.</p>
<div class="kpis">{kpis}</div><h2>Detected campaigns</h2><div class="kpis">{camp_kpis}</div>
<h2>Detector evaluation (injected campaigns)</h2>{eval_html}
<h2>Charts</h2><div class="charts">{charts}</div>
<h2>Most inflated products</h2>{_table(inflated)}
<h2>Largest campaigns</h2>{_table(campaigns) if campaigns is not None else ''}
<h2>Why reviews were flagged</h2>{_table(reasons)}
</main></body></html>"""
    out = res / "dashboard.html"
    out.write_text(page)
    return str(out)


def _kpi(label: str, value: str) -> str:
    return f"<div class='kpi'><b>{html.escape(value)}</b><span>{html.escape(label)}</span></div>"
