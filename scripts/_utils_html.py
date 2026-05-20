"""Shared HTML report rendering utilities."""
import html
from typing import Any

from ._utils_data import fmt_pct, fmt_money

# ---------------------------------------------------------------------------
# Dark theme CSS (shared across all analysis reports)
# ---------------------------------------------------------------------------
DARK_THEME_CSS = """
  :root {
    color-scheme: dark;
    --bg: #0f172a;
    --panel: #172033;
    --panel2: #1f2a3d;
    --line: #2d3b52;
    --text: #e5edf8;
    --muted: #93a4b8;
    --green: #22c55e;
    --red: #ef4444;
    --yellow: #f59e0b;
    --blue: #38bdf8;
  }
  * { box-sizing: border-box; }
  body { margin: 0; padding: 24px; background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
  h1 { margin: 0 0 6px; font-size: 24px; }
  h2 { margin: 28px 0 12px; font-size: 17px; color: #cbd5e1; }
  .sub { margin: 0 0 18px; color: var(--muted); font-size: 13px; }
  .note { margin: 0 0 20px; padding: 12px 14px; border: 1px solid #28456c; background: #10233d; color: #bfdbfe; border-radius: 8px; font-size: 13px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 18px; }
  .card { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 14px; }
  .label { color: var(--muted); font-size: 12px; margin-bottom: 5px; }
  .value { font-weight: 700; font-size: 22px; }
  .positive { color: var(--green); font-weight: 650; }
  .negative { color: var(--red); font-weight: 650; }
  .muted { color: var(--muted); }
  .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 14px; }
  .chart { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 14px; }
  .chart-title { color: #cbd5e1; font-size: 13px; font-weight: 650; margin-bottom: 10px; }
  .bar-row { display: flex; align-items: center; gap: 8px; margin: 7px 0; font-size: 12px; }
  .bar-label { width: 78px; color: var(--muted); text-align: right; flex: 0 0 auto; }
  .bar-track { height: 20px; background: #263448; border-radius: 5px; overflow: hidden; flex: 1; }
  .bar-fill { height: 100%; border-radius: 5px; }
  .bar-count { width: 82px; color: #cbd5e1; flex: 0 0 auto; }
  .table-wrap { overflow: auto; border: 1px solid var(--line); border-radius: 8px; background: var(--panel); }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th, td { padding: 8px 9px; border-bottom: 1px solid #243145; white-space: nowrap; text-align: left; }
  th { position: sticky; top: 0; background: var(--panel2); color: #b6c5d8; cursor: pointer; user-select: none; z-index: 1; }
  .compact th, .compact td { padding: 8px 10px; }
  tbody tr:nth-child(even) td { background: rgba(255, 255, 255, 0.018); }
  tbody tr:hover td { background: rgba(56, 189, 248, 0.07); }
  .addr { font-family: Consolas, monospace; color: #7dd3fc; user-select: all; }
  .narrative { max-width: 420px; overflow: hidden; text-overflow: ellipsis; }
  .toolbar { display: flex; gap: 10px; align-items: center; margin-bottom: 10px; }
  input { background: var(--panel); border: 1px solid var(--line); color: var(--text); border-radius: 7px; padding: 9px 10px; min-width: 280px; }
  footer { color: #64748b; font-size: 12px; margin-top: 28px; text-align: center; }
"""

# ---------------------------------------------------------------------------
# Common JS (sortable + searchable table)
# ---------------------------------------------------------------------------
TABLE_JS = """
  let sortState = {};
  function cellValue(row, index, numeric) {
    const cell = row.children[index];
    if (!cell) return numeric ? 0 : "";
    if (numeric) return parseFloat(cell.dataset.sort || cell.textContent.replace(/[$,%xKMSOL]/g, "")) || 0;
    return cell.textContent.trim().toLowerCase();
  }
  function sortTable(index, numeric) {
    const table = document.getElementById("detail");
    const tbody = table.tBodies[0];
    const rows = Array.from(tbody.rows);
    const key = index + ":" + numeric;
    const dir = sortState[key] === "asc" ? "desc" : "asc";
    sortState = { [key]: dir };
    rows.sort((a, b) => {
      const av = cellValue(a, index, numeric);
      const bv = cellValue(b, index, numeric);
      if (av < bv) return dir === "asc" ? -1 : 1;
      if (av > bv) return dir === "asc" ? 1 : -1;
      return 0;
    });
    rows.forEach(row => tbody.appendChild(row));
  }
  function filterRows() {
    const q = document.getElementById("search").value.trim().toLowerCase();
    document.querySelectorAll("#detail tbody tr").forEach(row => {
      row.style.display = row.dataset.search.includes(q) ? "" : "none";
    });
  }
"""


# ---------------------------------------------------------------------------
# Reusable HTML fragments
# ---------------------------------------------------------------------------

def bar_chart(items: list[tuple[str, int]], total: int, colors: list[str]) -> str:
    """Render a horizontal bar chart."""
    parts = []
    for index, (label, count) in enumerate(items):
        pct_val = count / total * 100 if total else 0
        color = colors[index % len(colors)]
        parts.append(f"""
        <div class="bar-row">
          <span class="bar-label">{html.escape(str(label))}</span>
          <div class="bar-track"><div class="bar-fill" style="width:{pct_val:.1f}%;background:{color}"></div></div>
          <span class="bar-count">{count} ({pct_val:.1f}%)</span>
        </div>
        """)
    return "\n".join(parts)


def stat_cards(items: list[tuple[str, str, str]]) -> str:
    """Render stat cards. Each item is (label, value, css_class)."""
    return "\n".join(
        f'<div class="card"><div class="label">{html.escape(label)}</div><div class="value {css_class}">{html.escape(value)}</div></div>'
        for label, value, css_class in items
    )


def group_table(title: str, groups: list[tuple[str, list[dict]]],
                stat_fn=None) -> str:
    """Render a group comparison table.

    Args:
        title: Section heading
        groups: List of (label, rows) pairs
        stat_fn: Function that takes rows and returns a dict with keys
                 count, hit30, hit100, alive, median_gain, median_current, median_drawdown
    """
    if stat_fn is None:
        from ._utils_data import group_stat as stat_fn

    rows_html = []
    for label, items in groups:
        stat = stat_fn(items)
        rows_html.append(f"""
        <tr>
          <td>{html.escape(str(label))}</td>
          <td>{stat['count']}</td>
          <td>{stat['hit30']:.1f}%</td>
          <td>{stat['hit100']:.1f}%</td>
          <td>{stat['alive']:.1f}%</td>
          <td class="positive">{fmt_pct(stat['median_gain'], signed=False)}</td>
          <td>{fmt_pct(stat['median_current'])}</td>
          <td class="negative">{fmt_pct(stat['median_drawdown'])}</td>
        </tr>
        """)
    return f"""
    <section>
      <h2>{html.escape(title)}</h2>
      <div class="table-wrap compact">
        <table>
          <thead>
            <tr>
              <th>分组</th><th>数量</th>
              <th>最高涨幅>=30%</th><th>最高涨幅>=100%</th>
              <th>当前仍上涨</th><th>最高涨幅中位</th>
              <th>当前涨跌中位</th><th>最大跌幅中位</th>
            </tr>
          </thead>
          <tbody>{''.join(rows_html)}</tbody>
        </table>
      </div>
    </section>
    """


def html_page(title: str, subtitle: str = "", note: str = "",
              body: str = "", footer: str = "") -> str:
    """Wrap body content in a full HTML page with dark theme."""
    sub_html = f'<p class="sub">{html.escape(subtitle)}</p>' if subtitle else ""
    note_html = f'<div class="note">{html.escape(note)}</div>' if note else ""
    footer_html = f"<footer>{html.escape(footer)}</footer>" if footer else ""
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>{DARK_THEME_CSS}</style>
</head>
<body>
  <h1>{html.escape(title)}</h1>
  {sub_html}
  {note_html}
  {body}
  {footer_html}
  <script>{TABLE_JS}</script>
</body>
</html>
"""
