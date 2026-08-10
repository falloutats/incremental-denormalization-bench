"""Final report: the summary table, the curves, and the honest costs.

Deliberately reports where the incremental engine LOSES as prominently as where it wins.
A demo that only shows the win is a sales pitch, and the interesting engineering question
is not "is this faster" but "under what conditions does this stop being faster".
"""

from __future__ import annotations

import argparse
import json
import os
import statistics

import plotext as plt
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .common import COLORS, ENGINES, human_bytes

LABEL = {"vanilla": "full refresh", "incremental": "incremental"}


def load(results: str, engine: str) -> tuple[list[dict], dict]:
    path = os.path.join(results, f"{engine}.jsonl")
    rows, meta = [], {}
    if not os.path.exists(path):
        return rows, meta
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        rec = json.loads(line)
        if rec.get("status") == "meta":
            meta = rec
        else:
            rows.append(rec)
    return rows, meta


def _vals(rows: list[dict], key: str) -> list[float]:
    """Post-backfill cycles only, and only the ones that completed.

    Tick 0 is the initial build: both engines construct the whole fact from scratch, so it
    is not a refresh cycle and averaging it in would say nothing about either engine's
    steady state.
    """
    return [r.get(key) or 0 for r in rows if r["tick"] > 0 and r["status"] == "ok"]


def steady(rows: list[dict], key: str) -> float:
    """Median of the post-backfill cycles -- the TYPICAL cycle.

    Reported alongside the mean and the worst cycle, never alone. Fan-out cycles are a
    small minority, so a median is guaranteed not to see them -- and those are exactly the
    cycles where the incremental engine loses. A headline that quietly excludes its own
    worst case is not a headline worth printing.
    """
    v = _vals(rows, key)
    return statistics.median(v) if v else 0.0


def mean(rows: list[dict], key: str) -> float:
    v = _vals(rows, key)
    return statistics.fmean(v) if v else 0.0


def worst(rows: list[dict], key: str) -> float:
    v = _vals(rows, key)
    return max(v) if v else 0.0


def chart(series: dict[str, tuple[list, list]], title: str, xlabel: str, ylabel: str,
          width: int = 100, height: int = 18) -> str:
    plt.clf()
    plt.plotsize(width, height)
    plt.theme("pro")
    for name, (xs, ys) in series.items():
        if len(xs) >= 2:
            plt.plot(xs, ys, label=LABEL.get(name, name),
                     color=COLORS.get(name, "white"), marker="braille")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    return plt.build()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="./results")
    args = ap.parse_args()
    console = Console()

    data = {e: load(args.results, e) for e in ENGINES}
    if not any(rows for rows, _ in data.values()):
        console.print("[red]no results found[/red]")
        return

    v_rows, v_meta = data["vanilla"]
    i_rows, i_meta = data["incremental"]

    # ---- fairness ----------------------------------------------------------
    fps = {e: (m.get("notes") or {}).get("conf_fingerprint") for e, (_, m) in data.items()}
    reported = {e: f for e, f in fps.items() if f}
    scale = (v_meta.get("notes") or {}).get("scale", "?")
    limit = v_meta.get("mem_limit_bytes") or (i_meta.get("mem_limit_bytes"))
    header = Text()
    header.append(f"scale={scale}   ")
    header.append(f"mem limit={human_bytes(limit) if limit else 'unset'}   ")
    header.append("spark conf: ")
    if len(reported) < len(ENGINES):
        missing = ", ".join(e for e in ENGINES if e not in reported)
        header.append(f"incomplete run — no metrics from {missing}", style="yellow")
    elif len(set(reported.values())) == 1:
        header.append("identical ✓", style="green bold")
    else:
        header.append(f"MISMATCH {reported} — comparison is void", style="red bold")
    console.print(Panel(header, title="run", border_style="cyan"))

    gate = os.path.join(args.results, "verify.json")
    if os.path.exists(gate):
        g = json.load(open(gate))
        console.print(Panel(
            Text("identical fact tables — the comparison is valid"
                 if g.get("identical") else f"FACT TABLES DIFFER: {g}",
                 style="green bold" if g.get("identical") else "red bold"),
            title="correctness gate", border_style="green" if g.get("identical") else "red"))

    # ---- summary -----------------------------------------------------------
    t = Table(title="per-cycle cost, post-backfill", expand=True)
    t.add_column("metric")
    for e in ENGINES:
        t.add_column(LABEL[e], justify="right")
    t.add_column("ratio", justify="right")

    def row(label, key, fmt, agg=steady):
        a, b = agg(v_rows, key), agg(i_rows, key)
        # No ratio unless both sides actually reported something -- a missing engine is
        # an incomplete run, not an infinite speedup.
        ratio = f"{a / b:,.2f}x" if a and b else "-"
        t.add_row(label, fmt(a), fmt(b), ratio)

    secs = lambda x: f"{x:,.2f}s"
    row("cycle time (typical / median)", "wall_s", secs)
    row("cycle time (mean, incl. fan-out)", "wall_s", secs, mean)
    row("cycle time (worst cycle)", "wall_s", secs, worst)
    row("bytes read", "bytes_read", human_bytes)
    # Both, never just the first. peak_rss_bytes is OCCUPANCY, and a long-lived JVM
    # never hands memory back, so that curve ratchets upward whatever the cycle actually
    # demanded -- reading it as cycle cost is the misreading metrics.py exists to warn
    # about. mem_peak_growth_bytes is the honest per-cycle number. Older runs predate the
    # field and report 0, which is why it is a separate row rather than a replacement.
    row("peak memory (occupancy)", "peak_rss_bytes", human_bytes)
    row("peak memory (growth this cycle)", "mem_peak_growth_bytes", human_bytes)
    row("local scratch written", "scratch_bytes", human_bytes)
    console.print(t)

    total_v = sum(r["wall_s"] for r in v_rows)
    total_i = sum(r["wall_s"] for r in i_rows)
    if total_v and total_i:
        console.print(f"cumulative compute: full refresh [red]{total_v:,.0f}s[/red] vs "
                      f"incremental [green]{total_i:,.0f}s[/green] "
                      f"([bold]{total_v / total_i:,.1f}x[/bold] over "
                      f"{min(len(v_rows), len(i_rows))} cycles)\n")

    # ---- curves ------------------------------------------------------------
    w = max(60, min(console.width - 4, 110))
    xs = {e: [r["fact_rows_total"] / 1e6 for r in rows] for e, (rows, _) in data.items()}
    console.print(chart({e: (xs[e], [r["wall_s"] for r in data[e][0]]) for e in ENGINES},
                        "cycle time vs table size", "fact rows (millions)", "seconds", w))
    console.print(chart({e: (xs[e], [(r["bytes_read"] or 0) / 1e6 for r in data[e][0]])
                         for e in ENGINES},
                        "bytes read per cycle (read amplification)",
                        "fact rows (millions)", "MB", w))
    console.print(chart({e: ([r["tick"] for r in data[e][0]],
                             [(r["peak_rss_bytes"] or 0) / 1e6 for r in data[e][0]])
                         for e in ENGINES},
                        f"memory OCCUPANCY vs the {human_bytes(limit) if limit else 'unset'} "
                        f"ceiling (ratchets with the JVM floor -- not cycle cost)",
                        "tick", "MB", w))
    console.print(chart({e: ([r["tick"] for r in data[e][0]],
                             [r.get("lag_s") or 0 for r in data[e][0]]) for e in ENGINES},
                        "freshness lag: SLA debt compounding", "tick", "seconds behind", w))

    # ---- where incremental loses -------------------------------------------
    losses = Table(title="where the incremental engine is WORSE", expand=True)
    losses.add_column("cost")
    losses.add_column("evidence", justify="right")

    idx_total = sum(r.get("index_maint_s") or 0 for r in i_rows)
    losses.add_row("secondary index maintenance",
                   f"{idx_total:,.1f}s ({idx_total / max(total_i, 1e-9):.0%} of its total time) "
                   f"— a tax the full refresh never pays")

    idx_bytes = max((r.get("notes") or {}).get("index_bytes", 0) for r in i_rows) if i_rows else 0
    losses.add_row("extra state on disk", f"{human_bytes(idx_bytes)} of indexes, plus a "
                                          f"checkpoint, that did not exist before")

    early = [r for r in i_rows if r["tick"] > 0][:3]
    if early and len(v_rows) > 3:
        ei = statistics.median([r["wall_s"] for r in early])
        ev = statistics.median([r["wall_s"] for r in v_rows[1:4]])
        if ei > ev:
            losses.add_row("cold start / small tables",
                           f"first cycles: {ei:,.1f}s vs {ev:,.1f}s — slower until the "
                           f"table is big enough to amortise the overhead")

    # No filtering on fan-out here: dimension updates happen on every real cycle, so
    # excluding them would report a number that never actually occurs.
    amps = [(r.get("notes") or {}).get("write_amplification", 0) for r in i_rows
            if r["tick"] > 0]
    amps = [a for a in amps if a]
    if amps:
        losses.add_row("partition-rewrite amplification",
                       f"{statistics.median(amps):,.1f}x — rows physically rewritten per row "
                       f"actually changed. Touching one row costs its whole partition, so "
                       f"partition width is now a tuning decision you did not have before.")

    fan = [r for r in i_rows if (r.get("fanout_rows") or 0) > 0]
    if fan:
        spike = max(fan, key=lambda r: r["wall_s"])
        vt = next((r["wall_s"] for r in v_rows if r["tick"] == spike["tick"]), None)
        by_table = (spike.get("notes") or {}).get("fanout_by_table", {})
        losses.add_row("dimension fan-out (write amplification)",
                       f"tick {spike['tick']}: changed dimensions {by_table} forced "
                       f"{spike['fanout_rows']:,} rows to be rebuilt across "
                       f"{(spike.get('notes') or {}).get('affected_partitions', '?')} partitions "
                       f"— {spike['wall_s']:,.1f}s"
                       + (f" vs {vt:,.1f}s for the full refresh" if vt else ""))

    parts = [(r.get("notes") or {}).get("source_partitions_read", {}) for r in i_rows
             if r["tick"] > 0]
    if parts and parts[-1]:
        ranked = sorted(parts[-1].items(), key=lambda kv: -kv[1])
        detail = ", ".join(f"{k} {v}" for k, v in ranked)
        losses.add_row("how well each dimension pruned",
                       f"partitions read on the last cycle: {detail}. A dimension whose "
                       f"rows are referenced uniformly across history cannot be narrowed by "
                       f"any index — that column should be a runtime join, not a "
                       f"denormalized one. This is the diagnostic to run on your own data.")

    losses.add_row("complexity",
                   "3 moving parts (silver feed, indexes, checkpointed merge) instead of 1, "
                   "plus a reconciliation gate to trust any of it")
    console.print(losses)

    # ---- breaking point ----------------------------------------------------
    for e in ENGINES:
        rows, _ = data[e]
        broke = [r for r in rows if r["status"] in ("oom", "error")]
        if broke:
            b = broke[0]
            console.print(Panel(
                Text(f"{LABEL[e]} BROKE at tick {b['tick']} with "
                     f"{b['fact_rows_total']:,} fact rows under a "
                     f"{human_bytes(limit) if limit else '?'} cap\n{b.get('error', '')}",
                     style="red bold"), title="breaking point", border_style="red"))

    # Deliberately NOT claimed as "spilling": spark.local.dir holds ordinary shuffle
    # output as well as spill, and Spark cleans it asynchronously. Reported as what it is.
    for e in ENGINES:
        sc = steady(data[e][0], "scratch_bytes")
        if sc > 0:
            console.print(f"[yellow]{LABEL[e]} leaves {human_bytes(sc)} per cycle in local "
                          f"scratch (shuffle + spill combined).[/yellow]")


if __name__ == "__main__":
    main()
