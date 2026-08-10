"""Live terminal dashboard. Tails both engines' JSONL streams and renders as they go.

Runs on the host, outside both containers, so it never competes for the CPU and memory
budget the engines are being measured under. In --parallel mode both streams advance at
once; in the default sequential mode the second engine's row simply stays on "waiting"
until its container starts.
"""

from __future__ import annotations

import argparse
import json
import os
import time

import plotext as plt
from rich.align import Align
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .common import COLORS, ENGINES, human_bytes


class Stream:
    """Incrementally tails one engine's JSONL file."""

    def __init__(self, path: str):
        self.path = path
        self.pos = 0
        self.rows: list[dict] = []
        self.meta: dict = {}

    def poll(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path) as f:
            f.seek(self.pos)
            for line in f:
                # A container is appending to this file while we read it, so the last
                # line can be half-written. Stop without advancing `pos`, and the next
                # poll re-reads it from the start once the newline lands.
                if not line.endswith("\n"):
                    break
                self.pos += len(line)
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("status") == "meta":
                    self.meta = rec.get("notes", {})
                else:
                    self.rows.append(rec)

    @property
    def last(self) -> dict | None:
        return self.rows[-1] if self.rows else None

    def series(self, key: str) -> list[float]:
        return [r.get(key) or 0 for r in self.rows]

    def finished(self) -> bool:
        last = self.last
        return bool(last and last.get("status") in ("oom", "error"))


def terminal_plot(streams: dict[str, Stream], ykey: str, title: str, ylabel: str,
                  width: int, height: int, xkey: str = "fact_rows_total",
                  scale: float = 1.0, log: bool = False) -> str:
    plt.clf()
    plt.plotsize(width, height)
    plt.theme("pro")
    # The x scaling and its label both follow from xkey. They used to be hardcoded to
    # millions-of-rows, which was right for every current caller and silently wrong for
    # any future one: passing xkey="tick" would have divided tick numbers by a million and
    # still labelled the axis "fact rows".
    row_count_axis = xkey.endswith("rows_total")
    xdiv = 1e6 if row_count_axis else 1.0
    xlabel = "fact rows (millions)" if row_count_axis else xkey

    any_data = False
    for name, s in streams.items():
        xs = [v / xdiv for v in s.series(xkey)]
        ys = [v * scale for v in s.series(ykey)]
        if len(xs) < 2:
            continue
        any_data = True
        plt.plot(xs, ys, label=name, color=COLORS[name], marker="braille")
    if not any_data:
        return "  collecting…"
    if log:
        plt.yscale("log")
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    return plt.build()


def status_table(streams: dict[str, Stream]) -> Table:
    t = Table(expand=True, box=None, pad_edge=False)
    t.add_column("engine", style="bold", width=12)
    t.add_column("tick", justify="right", width=6)
    t.add_column("cycle", justify="right", width=9)
    t.add_column("read", justify="right", width=10)
    t.add_column("peak mem", justify="right", width=11)
    t.add_column("fact rows", justify="right", width=12)
    t.add_column("lag", justify="right", width=9)
    t.add_column("state", width=10)

    for name, s in streams.items():
        last = s.last
        if not last:
            t.add_row(name, "-", "-", "-", "-", "-", "-", "waiting")
            continue

        limit = last.get("mem_limit_bytes")
        peak = last.get("peak_rss_bytes") or 0
        mem_txt = human_bytes(peak)
        if limit:
            frac = peak / limit
            mem_style = "red bold" if frac > 0.9 else "yellow" if frac > 0.75 else "white"
            mem_txt = Text(f"{human_bytes(peak)}/{human_bytes(limit)}", style=mem_style)

        state = last.get("status", "ok")
        state_txt = Text(state, style="red bold" if state in ("oom", "error") else "green")
        lag = last.get("lag_s") or 0
        lag_txt = Text(f"{lag:,.0f}s", style="red bold" if lag > 60 else
                       "yellow" if lag > 0 else "white")

        t.add_row(
            Text(name, style=COLORS[name]),
            str(last.get("tick")),
            f"{last.get('wall_s', 0):,.2f}s",
            human_bytes(last.get("bytes_read") or 0),
            mem_txt,
            f"{last.get('fact_rows_total') or 0:,}",
            lag_txt,
            state_txt,
        )
    return t


def verdict_panel(streams: dict[str, Stream]) -> Panel:
    """The honest summary: where each engine wins, and where the crossover sits."""
    v, i = streams["vanilla"], streams["incremental"]
    lines: list[Text] = []

    n = min(len(v.rows), len(i.rows))
    if n == 0:
        return Panel(Text("waiting for first cycle…"), title="verdict", border_style="dim")

    # Crossover: first tick after which incremental stays ahead.
    crossover = None
    for k in range(n):
        if v.rows[k]["wall_s"] > i.rows[k]["wall_s"]:
            if all(v.rows[j]["wall_s"] > i.rows[j]["wall_s"] for j in range(k, n)):
                crossover = k
                break

    if crossover is None:
        lines.append(Text("no crossover yet — full refresh is still ahead", style="yellow"))
    else:
        rows_at = v.rows[crossover]["fact_rows_total"]
        lines.append(Text(f"crossover at tick {crossover} ({rows_at:,} fact rows): "
                          f"incremental is faster from here on", style="green bold"))

    last_v, last_i = v.rows[n - 1], i.rows[n - 1]
    if last_i["wall_s"] > 0:
        speedup = last_v["wall_s"] / last_i["wall_s"]
        lines.append(Text(f"latest cycle: {speedup:,.2f}x  "
                          f"({last_v['wall_s']:,.1f}s vs {last_i['wall_s']:,.1f}s)"))
    read_v = last_v.get("bytes_read") or 0
    read_i = last_i.get("bytes_read") or 1
    lines.append(Text(f"read amplification: full refresh reads "
                      f"{read_v / max(read_i, 1):,.1f}x more per cycle "
                      f"({human_bytes(read_v)} vs {human_bytes(read_i)})"))

    idx_share = sum(r.get("index_maint_s") or 0 for r in i.rows) / max(
        sum(r["wall_s"] for r in i.rows), 1e-9)
    lines.append(Text(f"incremental overhead: {idx_share:.0%} of its total time is index "
                      f"maintenance the full refresh never pays", style="yellow"))

    fan = [r for r in i.rows if (r.get("fanout_rows") or 0) > 0]
    if fan:
        worst = max(fan, key=lambda r: r["wall_s"])
        vt = v.rows[worst["tick"]]["wall_s"] if worst["tick"] < len(v.rows) else None
        cmp = f" vs {vt:,.1f}s full refresh" if vt else ""
        # Every cycle has some fan-out -- cards are re-issued continuously -- so name the
        # dimensions responsible rather than assuming it was the scripted offer change.
        by_table = (worst.get("notes") or {}).get("fanout_by_table", {})
        lines.append(Text(f"fan-out spike: tick {worst['tick']} rebuilt "
                          f"{worst['fanout_rows']:,} rows from changed {by_table} — "
                          f"{worst['wall_s']:,.1f}s{cmp}", style="yellow"))

    for s, label in ((v, "full refresh"), (i, "incremental")):
        if s.finished():
            last = s.last
            lines.append(Text(f"{label} BROKE at tick {last['tick']} "
                              f"({last.get('fact_rows_total') or 0:,} rows): "
                              f"{last.get('status')} {last.get('error', '')[:60]}",
                              style="red bold"))

    return Panel(Group(*lines), title="verdict", border_style="cyan")


def build_layout(streams: dict[str, Stream], width: int, height: int) -> Layout:
    root = Layout()
    root.split_column(
        Layout(name="head", size=3),
        Layout(name="status", size=len(ENGINES) + 2),
        Layout(name="charts", ratio=1),
        Layout(name="verdict", size=9),
    )

    fp = {n: s.meta.get("conf_fingerprint") for n, s in streams.items() if s.meta}
    head = Text("full refresh  vs  incremental graph refresh", style="bold")
    head.append("    spark conf: ")
    if len(fp) < len(ENGINES):
        # Expected in the default sequential mode: the second engine has not started, so
        # there is nothing to compare yet. Only a MISMATCH below invalidates a run.
        head.append("waiting for both engines…", style="yellow")
    elif len(set(fp.values())) == 1:
        head.append("identical ✓", style="green")
    else:
        head.append(f"MISMATCH {fp}", style="red bold")
    root["head"].update(Panel(Align.center(head), border_style="dim"))

    root["status"].update(Panel(status_table(streams), border_style="dim", padding=0))

    cw = max(40, width // 2 - 4)
    ch = max(12, (height - 16) // 2)
    charts = Layout()
    charts.split_column(Layout(name="top"), Layout(name="bottom"))
    charts["top"].split_row(Layout(name="time"), Layout(name="mem"))
    charts["bottom"].split_row(Layout(name="read"), Layout(name="lag"))

    charts["top"]["time"].update(Panel(
        Text.from_ansi(terminal_plot(streams, "wall_s", "cycle time", "seconds", cw, ch)),
        title="cycle time vs dataset size", border_style="dim"))
    charts["top"]["mem"].update(Panel(
        Text.from_ansi(terminal_plot(streams, "peak_rss_bytes", "peak memory", "MB",
                                     cw, ch, scale=1 / 1e6)),
        title="peak memory vs the hard limit", border_style="dim"))
    charts["bottom"]["read"].update(Panel(
        Text.from_ansi(terminal_plot(streams, "bytes_read", "bytes read per cycle", "MB",
                                     cw, ch, scale=1 / 1e6)),
        title="read amplification", border_style="dim"))
    charts["bottom"]["lag"].update(Panel(
        Text.from_ansi(terminal_plot(streams, "lag_s", "freshness lag", "seconds", cw, ch)),
        title="freshness lag (SLA debt)", border_style="dim"))

    root["charts"].update(charts)
    root["verdict"].update(verdict_panel(streams))
    return root


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="./results")
    ap.add_argument("--refresh", type=float, default=1.0)
    ap.add_argument("--expect-ticks", type=int, default=None)
    args = ap.parse_args()

    console = Console()
    streams = {n: Stream(os.path.join(args.results, f"{n}.jsonl")) for n in ENGINES}

    with Live(console=console, refresh_per_second=4, screen=True) as live:
        idle = 0
        while True:
            before = {n: len(s.rows) for n, s in streams.items()}
            for s in streams.values():
                s.poll()
            progressed = any(len(streams[n].rows) > before[n] for n in ENGINES)
            idle = 0 if progressed else idle + 1

            live.update(build_layout(streams, console.width, console.height))

            done = all(
                s.finished() or (args.expect_ticks and len(s.rows) >= args.expect_ticks)
                for s in streams.values()
            )
            if done or idle > 120:
                break
            time.sleep(args.refresh)

    console.print(build_layout(streams, console.width, console.height))


if __name__ == "__main__":
    main()
