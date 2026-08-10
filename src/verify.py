"""Correctness gate: the two engines must produce the same fact table.

Razorpay ran "record-by-record reconciliation" and shadow A/B testing before letting the
incremental pipeline serve merchants. The demo version of that discipline is this file,
and it runs before any performance number is allowed to count. An incremental engine that
is faster because it quietly skipped work is not a result, it is a bug.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from .graph import fact_column_names


def compare_facts(spark: SparkSession, left_dir: str, right_dir: str,
                  left_name: str = "vanilla", right_name: str = "incremental") -> dict:
    cols = fact_column_names()
    left = spark.read.parquet(left_dir).select(*cols)
    right = spark.read.parquet(right_dir).select(*cols)

    n_left, n_right = left.count(), right.count()

    # The fact holds one row per payment, so rows != distinct ids means duplicates -- and
    # duplicates are the one defect exceptAll below cannot describe usefully. It reports
    # them as "extra rows on one side" with every value correct, which reads like a
    # mystery; this names it. A real bug hid behind exactly that ambiguity: two dimensions
    # fanning out to the same payment in one cycle rebuilt it twice.
    dup_left = n_left - left.select("id").distinct().count()
    dup_right = n_right - right.select("id").distinct().count()
    # exceptAll both ways catches missing rows, extra rows and wrong values alike.
    only_left = left.exceptAll(right)
    only_right = right.exceptAll(left)
    n_only_left, n_only_right = only_left.count(), only_right.count()

    result = {
        "identical": n_only_left == 0 and n_only_right == 0 and n_left == n_right,
        f"{left_name}_rows": n_left,
        f"{right_name}_rows": n_right,
        f"only_in_{left_name}": n_only_left,
        f"only_in_{right_name}": n_only_right,
        f"duplicate_ids_{left_name}": dup_left,
        f"duplicate_ids_{right_name}": dup_right,
    }

    if not result["identical"]:
        result["sample_only_left"] = [r.asDict() for r in only_left.limit(5).collect()]
        result["sample_only_right"] = [r.asDict() for r in only_right.limit(5).collect()]
        # A key present on both sides with different values is a logic bug in the
        # incremental merge; a key on one side only is a missed or spurious row.
        shared = (only_left.select("id").intersect(only_right.select("id"))).count()
        result["keys_with_differing_values"] = shared

    return result


def summarize(r: dict) -> str:
    if r["identical"]:
        return f"MATCH: both engines produced {r['vanilla_rows']:,} identical rows"
    dups = ""
    if r.get("duplicate_ids_vanilla") or r.get("duplicate_ids_incremental"):
        dups = (f" DUPLICATE ids: vanilla={r['duplicate_ids_vanilla']:,} "
                f"incremental={r['duplicate_ids_incremental']:,} "
                f"(a payment was rebuilt more than once in a cycle)")
    return (f"MISMATCH: vanilla={r['vanilla_rows']:,} incremental={r['incremental_rows']:,} "
            f"only_in_vanilla={r['only_in_vanilla']:,} "
            f"only_in_incremental={r['only_in_incremental']:,} "
            f"differing_keys={r.get('keys_with_differing_values', 0):,}" + dups)
