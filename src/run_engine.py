"""Container entrypoint: run one engine over the whole tick sequence.

One SparkSession is created once and reused for every tick. JVM startup is 5-10s and
would otherwise swamp the early cycles, which are exactly the ones where the incremental
engine is supposed to look bad -- measuring startup instead of work would flatter it.
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback

from .config import RESULTS_DIR, SCALES
from .incremental import IncrementalGraphRefresh
from .metrics import MetricsWriter, TickMetrics, memory_limit_bytes, oom_kill_count
from .pipeline import build_spark, conf_fingerprint, spark_conf
from .vanilla import VanillaFullRefresh

ENGINES = {"vanilla": VanillaFullRefresh, "incremental": IncrementalGraphRefresh}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, choices=sorted(ENGINES))
    ap.add_argument("--scale", default="demo", choices=sorted(SCALES))
    ap.add_argument("--lake", default="/lake")
    ap.add_argument("--out", default="/out")
    ap.add_argument("--results", default=RESULTS_DIR)
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--driver-mem", default="1500m")
    ap.add_argument("--shuffle-partitions", type=int, default=16)
    ap.add_argument("--local-dir", default="/tmp/spark")
    ap.add_argument("--ticks", type=int, default=None, help="override tick count")
    ap.add_argument("--tick-interval", type=float, default=None,
                    help="seconds a cycle may take before freshness lag accrues "
                         "(default: the scale's derived SLA slot, see config.py)")
    args = ap.parse_args()

    scale = SCALES[args.scale]
    n_ticks = args.ticks or scale.ticks
    # One tick is one simulated day; the slot is what that day is worth in wall clock.
    interval = args.tick_interval or scale.sla_slot_s
    origin = "overridden" if args.tick_interval else "derived from the day-zero refresh cost"
    print(f"[{args.engine}] SLA slot {interval:.1f}s/cycle = one simulated day "
          f"at scale {scale.name} ({origin})", flush=True)

    os.makedirs(args.local_dir, exist_ok=True)
    conf = spark_conf(args.threads, args.driver_mem, args.shuffle_partitions, args.local_dir)
    spark = build_spark(f"{args.engine}-{args.scale}", conf)

    writer = MetricsWriter(os.path.join(args.results, f"{args.engine}.jsonl"))
    engine = ENGINES[args.engine](spark, args.lake, args.out)

    # Fairness record. Both engines emit this; if the fingerprints differ, the two runs
    # were not run under the same rules and the comparison is void.
    meta = TickMetrics(tick=-1, engine=args.engine, status="meta")
    meta.notes = {
        "conf_fingerprint": conf_fingerprint(conf),
        "conf": conf,
        "scale": scale.name,
        "ticks": n_ticks,
        "tick_interval_s": interval,
    }
    meta.mem_limit_bytes = memory_limit_bytes()
    writer.write(meta)

    oom_before = oom_kill_count()
    lag = 0.0
    rc = 0

    for tick in range(n_ticks):
        try:
            m = engine.run_tick(tick)
        except Exception as exc:  # noqa: BLE001 -- the failure IS the measurement
            m = TickMetrics(tick=tick, engine=args.engine, status="error",
                            error=f"{type(exc).__name__}: {exc}",
                            mem_limit_bytes=memory_limit_bytes())
            traceback.print_exc(file=sys.stderr)
            rc = 1

        # Freshness lag: a cycle that overruns its slot pushes the next one late, and the
        # debt compounds. This is the mechanism behind Razorpay's 48-hour staleness.
        lag = max(0.0, lag + m.wall_s - interval)
        m.lag_s = lag
        # Seconds are box-specific; days behind are not, and "48 hours stale" is the claim
        # this chart exists to test. One slot is one simulated day, so the ratio is it.
        m.notes["lag_days"] = round(lag / interval, 3)

        if oom_kill_count() > oom_before:
            m.status = "oom"
            rc = 137

        writer.write(m)
        print(f"[{args.engine}] tick {m.tick:3d}  {m.wall_s:7.2f}s  "
              f"read={m.bytes_read/1e6:8.1f}MB  rows={m.fact_rows_total:>10,}  "
              f"peak={m.peak_rss_bytes/1e6:7.0f}MB  "
              f"lag={m.lag_s:6.1f}s ({m.notes['lag_days']:4.1f}d)  {m.status}",
              flush=True)

        if m.status in ("oom", "error"):
            # A full refresh that cannot finish is the result, not a crash to hide.
            print(f"[{args.engine}] stopping at tick {tick}: {m.status} {m.error}", flush=True)
            break

    writer.close()
    spark.stop()
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
