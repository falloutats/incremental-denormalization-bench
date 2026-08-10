"""Container entrypoint for the correctness gate."""

from __future__ import annotations

import argparse
import json
import os

from .pipeline import build_spark, spark_conf
from .verify import compare_facts, summarize


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--left", required=True)
    ap.add_argument("--right", required=True)
    ap.add_argument("--results", default="/results")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--driver-mem", default="3g")
    ap.add_argument("--local-dir", default="/tmp/spark")
    args = ap.parse_args()

    os.makedirs(args.local_dir, exist_ok=True)
    spark = build_spark("verify", spark_conf(args.threads, args.driver_mem, 32, args.local_dir))

    result = compare_facts(spark, args.left, args.right)
    print(summarize(result), flush=True)

    os.makedirs(args.results, exist_ok=True)
    with open(os.path.join(args.results, "verify.json"), "w") as f:
        json.dump(result, f, indent=2)

    spark.stop()
    return 0 if result["identical"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
