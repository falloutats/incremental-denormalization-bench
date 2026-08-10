"""Sensitivity sweep over `Scale.tail_update_rate` -- the knob the whole result rests on.

config.py claims, in prose, that 0.15% of daily volume arriving as back-dated corrections
is a defensible default and that "push the rate to ~1% and ... the incremental engine does
strictly more work than the full refresh it replaced". Nobody had measured either half of
that sentence. This module replaces the argument with a curve, so a reader who knows their
own correction rate can find it on the x-axis and read off whether the technique pays.

Two halves, deliberately separated because they cost four orders of magnitude apart:

  locality (cheap)   What fraction of fact partitions does one cycle dirty? This is a pure
                     property of the generated data -- no Spark, no lake on disk, seconds
                     for the whole grid. It is the mechanism: a partition-scoped upsert can
                     only save work if the changed rows sit in a MINORITY of partitions,
                     and this is the number that says whether they do.
  speed (expensive)  Does that mechanism actually show up in wall clock and bytes? For a
                     handful of rates, generate a real lake and race both engines through
                     it. This is the half that can disagree with the theory, which is why
                     it exists.

Nothing here changes config.py, and nothing here changes either engine. The only input
that varies between runs is the generated data, via
`dataclasses.replace(scale, tail_update_rate=r)` -- Scale is frozen precisely so this is
the only way to do it. The engines are imported and driven as-is; if a number here is
wrong it is wrong in the same way the demo's own numbers are wrong.

What it found, on a 12-core M-series laptop, demo scale, 10 ticks (the JSON is the record;
these are the two sentences worth carrying around):

  * Break-even is tail_update_rate ~= 0.0075, about 0.75% of daily volume and 5x the
    shipped default. The default is therefore inside the winning regime rather than at its
    edge -- but the margin is 5x, not the 3 orders of magnitude an auditor might assume,
    and the curve at the default is already falling (1.37x at 0.0001, 1.26x at 0.0015).
  * The tail rate is NOT the only thing dirtying partitions, and below ~0.002 it is not
    even the main one. At the default the locality model says 23% of partitions are
    touched; the engine actually rewrites 40%. The gap is card re-issues -- 25 rows a
    cycle, scattered across all history by construction -- which dirty ~70 partitions on
    their own no matter what the tail rate is. `card_updates` deserves a sweep of its own.

Run:

    source ./env.sh
    python -m src.sweep                      # both halves, default grid
    python -m src.sweep --locality-only      # seconds, no Spark
    python -m src.sweep --report-only R.json # re-render a finished run
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
import os
import shutil
import statistics
import time

import plotext as plt

from dashboard.common import COLORS, human_bytes

from .config import SCALES, Scale
from .generate import Generator, LakeWriter
from .incremental import IncrementalGraphRefresh
from .metrics import TickMetrics
from .pipeline import build_spark, conf_fingerprint, spark_conf
from .vanilla import VanillaFullRefresh
from .verify import compare_facts

# --- the grids --------------------------------------------------------------
# The auditor's claim is that real correction rates are "1-3 orders of magnitude" above
# the shipped 0.0015, so the grid has to cover 0.0001 (an order of magnitude BELOW the
# default, to show the flat end) up to 0.02 (an order of magnitude above), with enough
# points per decade that the shape is not an artifact of where the samples landed.
LOCALITY_RATES = (0.0001, 0.0002, 0.0003, 0.0005, 0.0008, 0.001, 0.0015,
                  0.002, 0.003, 0.005, 0.008, 0.01, 0.015, 0.02)

# The expensive half gets six points rather than fourteen: each one is a full lake plus
# two engine runs. Chosen to bracket the interesting region rather than to space evenly --
# the shipped default is in the list so the sweep says something directly about it, and
# 0.004/0.008 sit where the locality curve predicts the crossover should be.
SPEED_RATES = (0.0001, 0.0005, 0.0015, 0.004, 0.008, 0.02)

SCRATCH = ("/private/tmp/claude-501/-Users-fallout-razorpay-demo/"
           "cca109e2-0b81-43ca-853f-e3068d75d24b/scratchpad")


# --- the cheap half: partition locality -------------------------------------

class _NullLakeWriter(LakeWriter):
    """A LakeWriter that writes nothing.

    The locality question is answered entirely by the generator's own bookkeeping --
    `Generator.tick()` already returns distinct_update_partitions / total_partitions --
    so the parquet is pure cost. Swapping the writer out (rather than reimplementing
    _sample_updates here) means this half measures the SAME sampling code the expensive
    half runs, and cannot drift away from it.
    """

    def __init__(self) -> None:
        super().__init__(root="")

    def write_partitioned(self, *a, **k) -> None:  # noqa: D102
        pass

    def write_unpartitioned(self, *a, **k) -> None:  # noqa: D102
        pass

    def write_silver(self, *a, **k) -> None:  # noqa: D102
        pass


def locality_point(scale: Scale, rate: float) -> dict:
    """Simulate the whole tick sequence at `rate` and report how concentrated the churn is.

    Reported two ways on purpose. The MEDIAN over post-backfill ticks is the number that
    belongs on a curve; the LAST tick is the one to quote when arguing about a mature
    table, since the denominator (total partitions) grows by one every tick and the
    fraction therefore drifts down slightly over a run.
    """
    scale = dataclasses.replace(scale, tail_update_rate=rate)
    gen = Generator(scale, root="<locality: nothing is written>")
    gen.w = _NullLakeWriter()

    stats = [gen.tick(t) for t in range(scale.ticks)]
    post = stats[1:]  # tick 0 is the backfill; it updates nothing by construction
    fracs = [s["distinct_update_partitions"] / max(s["total_partitions"], 1) for s in post]
    return {
        "rate": rate,
        "scale": scale.name,
        # int() because that is exactly what _sample_updates does -- at smoke scale a rate
        # of 0.0001 buys you two corrections, not 2.0, and the quantisation is visible.
        "tail_updates_per_tick": int(rate * scale.new_per_tick),
        "updated_rows_last": post[-1]["updated_payments"],
        "dirty_partitions_last": post[-1]["distinct_update_partitions"],
        "total_partitions_last": post[-1]["total_partitions"],
        "dirty_frac_last": fracs[-1],
        "dirty_frac_median": statistics.median(fracs),
    }


def locality_curve(scale_names: list[str], rates: tuple[float, ...]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for name in scale_names:
        t0 = time.perf_counter()
        out[name] = [locality_point(SCALES[name], r) for r in rates]
        print(f"  locality {name:7s} {len(rates)} rates in {time.perf_counter() - t0:.1f}s",
              flush=True)
    return out


# --- the expensive half: actual cycle time and bytes ------------------------

def _run_engine(cls, spark, lake: str, out: str, ticks: int) -> list[TickMetrics]:
    """One engine, all ticks, on an already-generated lake.

    Deliberately NOT run_engine.main(): that builds its own SparkSession, and the whole
    sweep has to share one because a smoke-to-demo cycle is 15-20s and a JVM start is
    5-10s. Everything else -- the engine classes, the tick loop, the metrics -- is the
    repo's own.
    """
    engine = cls(spark, lake, out)
    return [engine.run_tick(t) for t in range(ticks)]


def _summarise(metrics: list[TickMetrics]) -> dict:
    """Post-backfill medians.

    Tick 0 is the backfill: both engines build the whole fact from scratch and neither is
    doing the thing being measured, so it is excluded from every headline number and kept
    only in the raw per-tick dump. The median (not the mean) is the summary because the
    scripted fan-out tick is a genuine several-fold spike that would otherwise decide the
    comparison on its own -- it is reported separately as `max_s` instead.
    """
    post = [m for m in metrics[1:] if m.status == "ok"]
    return {
        "median_s": statistics.median(m.wall_s for m in post),
        "max_s": max(m.wall_s for m in post),
        "total_s": sum(m.wall_s for m in metrics),
        "median_bytes_read": statistics.median(m.bytes_read for m in post),
        "median_rows_rewritten": statistics.median(m.rows_rewritten for m in post),
        "median_rows_written": statistics.median(m.rows_written for m in post),
        "fact_rows_final": metrics[-1].fact_rows_total,
        "ticks_ok": len(post),
    }


def speed_point(spark, scale: Scale, rate: float, work: str, ticks: int,
                gate: bool, keep: bool) -> dict:
    """Generate a lake at `rate`, race both engines over it, optionally run the gate."""
    scale = dataclasses.replace(scale, tail_update_rate=rate, ticks=ticks)
    tag = f"r{rate:g}".replace(".", "")
    lake = os.path.join(work, f"lake_{tag}")
    outs = {"vanilla": os.path.join(work, f"out_v_{tag}"),
            "incremental": os.path.join(work, f"out_i_{tag}")}
    for d in (lake, *outs.values()):
        shutil.rmtree(d, ignore_errors=True)

    t0 = time.perf_counter()
    manifest = Generator(scale, lake).run()
    gen_s = time.perf_counter() - t0
    print(f"  [{rate:g}] lake: {manifest['total_payments']:,} payments, "
          f"{manifest['tail_updates_per_tick']} tail updates/tick, {gen_s:.1f}s", flush=True)

    result: dict = {"rate": rate, "gen_s": gen_s, "per_tick": {}}
    for name, cls in (("vanilla", VanillaFullRefresh), ("incremental", IncrementalGraphRefresh)):
        ms = _run_engine(cls, spark, lake, outs[name], ticks)
        result[name] = _summarise(ms)
        result["per_tick"][name] = [dataclasses.asdict(m) for m in ms]
        print(f"  [{rate:g}] {name:11s} median {result[name]['median_s']:6.2f}s  "
              f"read {human_bytes(result[name]['median_bytes_read']):>9}  "
              f"total {result[name]['total_s']:6.1f}s", flush=True)

    # Ratios in the direction a reader expects: >1 means incremental is the better one.
    result["speedup"] = result["vanilla"]["median_s"] / result["incremental"]["median_s"]
    result["bytes_ratio"] = (result["vanilla"]["median_bytes_read"]
                             / max(result["incremental"]["median_bytes_read"], 1))
    result["write_amplification"] = (result["incremental"]["median_rows_rewritten"]
                                     / max(result["incremental"]["median_rows_written"], 1))

    if gate:
        # The gate is the reason any of these timings mean anything. A sweep that varied
        # the data until the incremental engine silently stopped producing correct output
        # would show a beautiful, meaningless curve.
        t0 = time.perf_counter()
        verdict = compare_facts(spark,
                                os.path.join(outs["vanilla"], "payments_fact"),
                                os.path.join(outs["incremental"], "payments_fact"))
        verdict["gate_s"] = time.perf_counter() - t0
        result["gate"] = verdict
        print(f"  [{rate:g}] gate: {'MATCH' if verdict['identical'] else 'MISMATCH'} on "
              f"{verdict['vanilla_rows']:,} rows ({verdict['gate_s']:.0f}s)", flush=True)

    if not keep:
        # A demo-scale lake plus two fact tables is ~600MB per rate. Six of those left
        # lying around is how a sweep fills a laptop.
        for d in (lake, *outs.values()):
            shutil.rmtree(d, ignore_errors=True)
    return result


# --- the headline -----------------------------------------------------------

def crossing(points: list[tuple[float, float]], level: float = 1.0) -> float | None:
    """Rate at which a monotonically-falling curve crosses `level`, log-interpolated.

    Log-linear because the x axis spans two decades: interpolating linearly between
    0.004 and 0.008 would put the answer in the wrong place by a wide margin. Returns
    None when the curve never crosses, which is itself an answer worth printing.
    """
    for (x0, y0), (x1, y1) in zip(points, points[1:]):
        if (y0 - level) * (y1 - level) <= 0 and y0 != y1:
            f = (y0 - level) / (y0 - y1)
            return 10 ** (math.log10(x0) + f * (math.log10(x1) - math.log10(x0)))
    return None


# --- output -----------------------------------------------------------------

def print_locality_table(curve: dict[str, list[dict]], default_rate: float) -> None:
    scales = list(curve)
    print("\n=== partition locality: fraction of fact partitions dirtied per cycle ===")
    print("(median over post-backfill ticks; the upsert degenerates into a full rewrite "
          "as this approaches 100%)\n")
    head = f"{'tail_rate':>10} " + "".join(f"{s:>22}" for s in scales)
    print(head)
    print(f"{'':>10} " + "".join(f"{'tail/tick  dirty':>22}" for _ in scales))
    print("-" * len(head))
    for i, rate in enumerate([p["rate"] for p in curve[scales[0]]]):
        mark = "  <- shipped default" if abs(rate - default_rate) < 1e-12 else ""
        row = f"{rate:>10.4f} "
        for s in scales:
            p = curve[s][i]
            row += f"{p['tail_updates_per_tick']:>13,} {p['dirty_frac_median']:>7.1%}"
        print(row + mark)


def measured_dirty_frac(point: dict, backfill_days: int) -> float:
    """What fraction of fact partitions the engine ACTUALLY rewrote, per cycle.

    The locality half above counts only the partitions of changed PAYMENTS. The engine
    also rebuilds every payment reached by a changed dimension -- 25 card re-issues a tick,
    each scattered anywhere in history -- and those partitions are dirty too. Reading this
    next to dirty_frac_median is how you find out how much of the rewrite the tail rate is
    actually responsible for, which turns out to be the crux of the whole dispute.
    """
    fracs = []
    for m in point["per_tick"]["incremental"][1:]:
        affected = m["notes"].get("affected_partitions")
        if affected is None:
            continue
        fracs.append(affected / (backfill_days + m["tick"]))
    return statistics.median(fracs) if fracs else float("nan")


def paired_speedup(point: dict) -> float:
    """Median of the per-tick ratio, rather than the ratio of the two medians.

    Both engines see the same table at tick t, and cycle time climbs steadily with table
    size across a run -- at demo scale the last cycle costs ~1.8x the first. Dividing one
    engine's median by the other's therefore compares two numbers taken from the middle of
    two different curves, and any wobble in which tick lands in the middle shows up as a
    change in the answer. Pairing tick-for-tick removes the growth trend from the ratio
    entirely, so this is the estimator to trust; `speedup` is kept alongside it because it
    is the one a reader would compute by hand from the two medians.
    """
    v = [m["wall_s"] for m in point["per_tick"]["vanilla"][1:]]
    i = [m["wall_s"] for m in point["per_tick"]["incremental"][1:]]
    return statistics.median(a / b for a, b in zip(v, i))


def median_files(point: dict, engine: str) -> float:
    """Median post-backfill file count opened per cycle.

    Worth a column of its own because bytes turned out to be the wrong unit here. A
    back-dated correction lands in its own partition, and the generator writes one file per
    partition per tick -- so raising the tail rate multiplies the number of tiny files in
    the lake while barely moving its size. The full refresh opens every one of them.
    """
    return statistics.median(m["files_read"] for m in point["per_tick"][engine][1:])


def print_speed_table(points: list[dict], scale_name: str, ticks: int,
                      backfill_days: int) -> None:
    print(f"\n=== measured cycle cost ({scale_name} scale, {ticks} ticks, "
          f"post-backfill medians) ===\n")
    print(f"{'tail_rate':>10} {'vanilla':>9} {'incr':>9} {'paired':>8} {'speedup':>8} "
          f"{'v bytes':>10} {'i bytes':>10} {'byte adv':>9} "
          f"{'v files':>9} {'i files':>9} {'wr ampl':>8} {'dirty':>7} {'gate':>9}")
    print("-" * 127)
    for p in points:
        gate = p.get("gate")
        gate_s = "-" if gate is None else ("MATCH" if gate["identical"] else "MISMATCH")
        print(f"{p['rate']:>10.4f} {p['vanilla']['median_s']:>8.2f}s "
              f"{p['incremental']['median_s']:>8.2f}s {paired_speedup(p):>7.2f}x "
              f"{p['speedup']:>7.2f}x "
              f"{human_bytes(p['vanilla']['median_bytes_read']):>10} "
              f"{human_bytes(p['incremental']['median_bytes_read']):>10} "
              f"{p['bytes_ratio']:>8.2f}x "
              f"{median_files(p, 'vanilla'):>9,.0f} {median_files(p, 'incremental'):>9,.0f} "
              f"{p['write_amplification']:>7.1f}x "
              f"{measured_dirty_frac(p, backfill_days):>6.0%} {gate_s:>9}")
    print("\npaired = median of the per-tick ratio (the estimator to trust); speedup = ratio "
          "of the two\n         medians. They disagree by a few percent, which is the honest "
          "width of this measurement."
          "\ndirty = fact partitions the incremental engine actually rewrote per cycle, as a "
          "share of\n        all fact partitions. Compare with the locality table: the gap "
          "is the churn that\n        does NOT come from tail_update_rate at all."
          "\nfiles = parquet files opened per cycle. Watch this rather than bytes: the tail "
          "rate barely\n        changes how much data exists, but it multiplies how many "
          "files it is scattered over.")


def chart_locality(curve: dict[str, list[dict]], default_rate: float) -> None:
    plt.clear_figure()
    palette = ("cyan", "magenta", "orange")
    for i, (name, pts) in enumerate(curve.items()):
        plt.plot([p["rate"] for p in pts], [100 * p["dirty_frac_median"] for p in pts],
                 marker="braille", color=palette[i % len(palette)], label=name)
    plt.xscale("log")
    plt.hline(50)
    plt.vline(default_rate)
    plt.plotsize(96, 22)
    plt.theme("clear")
    plt.title(f"partition locality: % of fact partitions dirtied per cycle "
              f"(vline = shipped default {default_rate})")
    plt.xlabel("tail_update_rate")
    plt.show()


def chart_speed(points: list[dict], default_rate: float, breakeven: float | None) -> None:
    plt.clear_figure()
    rates = [p["rate"] for p in points]
    plt.plot(rates, [paired_speedup(p) for p in points], marker="braille",
             color=COLORS["incremental"], label="cycle-time speedup (vanilla/incremental)")
    plt.plot(rates, [p["bytes_ratio"] for p in points], marker="braille",
             color=COLORS["vanilla"], label="bytes-read advantage")
    plt.xscale("log")
    plt.hline(1.0)          # break-even: below this line the technique is a net loss
    plt.vline(default_rate)
    if breakeven:
        plt.vline(breakeven)
    plt.plotsize(96, 22)
    plt.theme("clear")
    plt.title("incremental advantage vs. tail_update_rate (1.0 = no better than full refresh)")
    plt.xlabel("tail_update_rate")
    plt.show()


# --- entrypoint -------------------------------------------------------------

def report(results: dict) -> None:
    """Print every table and chart from a results dict.

    Split out from main() so a finished sweep can be re-rendered from its JSON
    (`--report-only`) without paying half an hour to recompute it. That is not a
    convenience: the raw numbers are the artifact, and anything that can only be seen by
    re-running the measurement is not really evidence.
    """
    curve = results["locality"]
    # Older result files recorded the scale only under "speed"; accept both.
    base = SCALES[results.get("scale") or results["speed"]["scale"]]
    default_rate = results["shipped_default_rate"]

    print_locality_table(curve, default_rate)
    chart_locality(curve, default_rate)

    loc_50 = None
    if base.name in curve:
        loc_50 = crossing([(p["rate"], p["dirty_frac_median"]) for p in curve[base.name]], 0.5)
        print(f"locality: half of all fact partitions are dirty every cycle at "
              f"tail_update_rate ~= {loc_50:.4f} ({base.name} scale). Past that the "
              f"'scoped' upsert is a full rewrite wearing a hat.")
    results["locality_50pct_rate"] = loc_50

    if "speed" not in results:
        return
    points = results["speed"]["points"]
    print_speed_table(points, base.name, results["speed"]["ticks"], base.backfill_days)

    # Two estimators, deliberately both reported. Quoting a single break-even to four
    # decimals from six measured points would be false precision; the spread between them
    # is the honest uncertainty and it is about half a decade wide.
    breakeven = crossing([(p["rate"], p["speedup"]) for p in points])
    breakeven_paired = crossing([(p["rate"], paired_speedup(p)) for p in points])
    chart_speed(points, default_rate, breakeven_paired)
    results["breakeven_rate"] = breakeven
    results["breakeven_rate_paired"] = breakeven_paired

    print("\n=== verdict ===")
    at_default = min(points, key=lambda p: abs(p["rate"] - default_rate))
    print(f"at the shipped default ({default_rate}): "
          f"{paired_speedup(at_default):.2f}x cycle time, "
          f"{at_default['bytes_ratio']:.2f}x bytes, "
          f"{measured_dirty_frac(at_default, base.backfill_days):.0%} of partitions "
          f"rewritten per cycle")
    both = [b for b in (breakeven_paired, breakeven) if b]
    if not both:
        print(f"the curve never crosses 1.0 over {points[0]['rate']:g}"
              f"..{points[-1]['rate']:g} ({points[0]['speedup']:.2f}x -> "
              f"{points[-1]['speedup']:.2f}x): over this range the answer does not depend "
              f"on the tail rate the way config.py claims it does.")
    else:
        lo, hi = min(both), max(both)
        print(f"BREAK-EVEN: the incremental engine stops paying for itself at "
              f"tail_update_rate ~= {lo:.4f}-{hi:.4f} "
              f"({lo * 100:.2f}%-{hi * 100:.2f}% of daily volume), i.e. "
              f"{lo / default_rate:.1f}-{hi / default_rate:.1f}x the shipped default.")
        print(f"Take the conservative end -- {lo * 100:.1f}% of daily volume -- as the "
              f"planning number. Measured over {results['speed']['ticks']} ticks; the "
              f"per-tick ratios rise as the table grows, so a longer run would put the "
              f"crossing later, not earlier.")

    bad = [p for p in points if p.get("gate") and not p["gate"]["identical"]]
    if bad:
        print(f"\nGATE: {len(bad)} of the gated points did NOT match. Every timing above "
              f"was produced by an engine that is not exactly correct -- see the gate "
              f"detail in the JSON before quoting any of it.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--report-only", default=None, metavar="JSON",
                    help="re-render tables and charts from a previous run's JSON")
    ap.add_argument("--scale", default="demo", choices=sorted(SCALES),
                    help="scale for the MEASURED half. demo by default, not smoke: at "
                         "smoke scale the incremental engine loses at every rate "
                         "(README: 0.59x), so there is no crossing to find")
    ap.add_argument("--locality-scales", default="smoke,demo,stress",
                    help="scales for the free half; it costs seconds, so run them all")
    ap.add_argument("--ticks", type=int, default=10,
                    help="ticks per engine run. 10 keeps six rate points inside ~35min "
                         "and still includes the scale's first fan-out tick")
    ap.add_argument("--rates", default=None,
                    help="comma-separated tail_update_rates for the measured half")
    ap.add_argument("--locality-only", action="store_true",
                    help="skip Spark entirely; prints the locality curve in seconds")
    ap.add_argument("--gate", default="ends", choices=("ends", "all", "none"),
                    help="run the correctness gate at the first+last rate ('ends'), at "
                         "every rate, or never. Never is not a serious option")
    ap.add_argument("--session", default="per-rate", choices=("per-rate", "shared"),
                    help="one SparkSession per rate point (default) or one for the whole "
                         "sweep. Shared is cheaper -- ~10s of JVM start per point -- but "
                         "at demo scale it drifts and then dies; see the comment below")
    ap.add_argument("--work", default=os.path.join(SCRATCH, "sweep"),
                    help="scratch directory for lakes and fact tables")
    ap.add_argument("--keep-lakes", action="store_true",
                    help="do not delete each rate's lake after measuring it (~600MB each)")
    ap.add_argument("--json", default=None, help="where to write raw results")
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--driver-mem", default="1400m")
    ap.add_argument("--shuffle-partitions", type=int, default=16)
    args = ap.parse_args()

    if args.report_only:
        with open(args.report_only) as f:
            saved = json.load(f)
        report(saved)
        with open(args.report_only, "w") as f:
            json.dump(saved, f, indent=2, default=str)
        print(f"\nraw results: {args.report_only}")
        return 0

    base = SCALES[args.scale]
    rates = (tuple(float(x) for x in args.rates.split(",")) if args.rates else SPEED_RATES)
    loc_scales = [s.strip() for s in args.locality_scales.split(",") if s.strip()]
    os.makedirs(args.work, exist_ok=True)
    json_path = args.json or os.path.join(args.work, "sweep_results.json")

    print(f"=== tail_update_rate sweep ===\nshipped default: {base.tail_update_rate} "
          f"({base.tail_update_rate:.2%} of daily volume, "
          f"{int(base.tail_update_rate * base.new_per_tick)} corrections/tick at "
          f"{args.scale} scale)\n")

    print("locality (no Spark):", flush=True)
    curve = locality_curve(loc_scales, LOCALITY_RATES)

    results: dict = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "scale": args.scale,
        "shipped_default_rate": base.tail_update_rate,
        "locality_rates": list(LOCALITY_RATES),
        "locality": curve,
    }

    if not args.locality_only:
        # spark_conf() is the repo's, unmodified, so both engines here run under exactly
        # the settings run_engine.py would give them. The single added key turns off the
        # console progress bar, which is cosmetic and would otherwise bury the sweep's own
        # output under thousands of stage lines.
        conf = spark_conf(args.threads, args.driver_mem, args.shuffle_partitions,
                          os.path.join(args.work, "sparktmp"))
        os.makedirs(conf["spark.local.dir"], exist_ok=True)
        session_conf = dict(conf, **{"spark.ui.showConsoleProgress": "false"})

        # See --session: one JVM for the whole sweep is the obvious choice and it is wrong
        # here. Measured: it OOMed part-way through the fourth rate point, and before that
        # the vanilla median drifted 15.6s -> 16.0s -> 17.1s across three points whose
        # input differed by 0.4% in bytes. That drift is JVM state, and it lands unevenly
        # -- vanilla runs first at each point, on a cleaner heap than the incremental run
        # that follows it -- so it biases the ratio the sweep exists to measure.
        shared = build_spark("tail-rate-sweep", session_conf) if args.session == "shared" else None

        points = []
        t0 = time.perf_counter()
        for i, rate in enumerate(rates):
            gate = (args.gate == "all"
                    or (args.gate == "ends" and i in (0, len(rates) - 1)))
            print(f"\n--- rate {rate:g} ({i + 1}/{len(rates)}, "
                  f"{time.perf_counter() - t0:.0f}s elapsed) ---", flush=True)
            spark = shared or build_spark(f"tail-rate-sweep-{rate:g}", session_conf)
            try:
                points.append(speed_point(spark, base, rate, args.work, args.ticks, gate,
                                          args.keep_lakes))
            finally:
                if shared is None:
                    spark.stop()
            # Written after every point: a sweep that dies at rate five should not throw
            # away the four rates it already paid for.
            results["speed"] = {
                "scale": args.scale, "ticks": args.ticks, "session": args.session,
                "conf_fingerprint": conf_fingerprint(conf),
                "conf": session_conf, "points": points,
            }
            with open(json_path, "w") as f:
                json.dump(results, f, indent=2, default=str)
        if shared is not None:
            shared.stop()

    report(results)
    with open(json_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nraw results: {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
