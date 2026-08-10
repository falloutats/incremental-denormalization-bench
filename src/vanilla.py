"""The baseline: rebuild the whole denormalised table, every cycle.

From the blog, describing what Razorpay ran before the rewrite: "The job then reads all
source tables from the data lake, performs a full Spark join, and overwrites the entire
denormalised table back to S3."

That is all this file does, and it matters that it stays this short. This is not a
strawman -- it is the correct, obvious implementation, the one most teams ship and run
happily for a year. Same Spark config, same parquet layout, same dedup helper, same
reader, same graph as the incremental engine. Adaptive execution and broadcast joins are
on. Nothing is sabotaged.

Its only property is that its cost is a function of how much data EXISTS, not of how much
data CHANGED. Every curve on the dashboard follows from that one property.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .graph import FACT_COLUMNS, FACT_GRAPH, fact_column_names, join_keys_needed
from .metrics import (
    MemorySampler,
    TickMetrics,
    Timer,
    dir_size,
    memory_limit_bytes,
    parquet_row_count,
)
from .pipeline import FactPipeline, latest_by_pk, write_fact


def build_fact(src: dict[str, DataFrame]) -> DataFrame:
    """Forward walk of the fact graph: root outward, one left join per edge.

    Shared with the incremental engine, which calls it on a small slice of the same
    tables. Identical construction on both sides is what makes the correctness gate
    meaningful -- if the outputs differ, it is the scheduling that differs, not the SQL.
    """
    g = FACT_GRAPH
    root_cols = list(dict.fromkeys(FACT_COLUMNS[g.root] + join_keys_needed(g.root)))
    out = src[g.root].select(*root_cols)

    for edge in g.forward_order():
        child_cols = list(dict.fromkeys(FACT_COLUMNS[edge.child] + join_keys_needed(edge.child)))
        child = src[edge.child].select(
            F.col(edge.child_col).alias("_jk"),
            *[F.col(c).alias(f"{edge.child}_{c}") for c in child_cols],
        )
        out = out.join(child, out[edge.parent_col] == F.col("_jk"), "left").drop("_jk")
        # A child's own outgoing join keys arrived prefixed; strip the prefix so the next
        # edge in the walk can find them by their bare name (discounts_offer_id -> offer_id).
        for onward in g.children_of(edge.child):
            out = out.withColumnRenamed(f"{edge.child}_{onward.parent_col}", onward.parent_col)

    return out.select(*fact_column_names())


class VanillaFullRefresh(FactPipeline):
    name = "vanilla"

    def run_tick(self, tick: int) -> TickMetrics:
        m = TickMetrics(tick=tick, engine=self.name, mem_limit_bytes=memory_limit_bytes())
        self._begin_tick(tick)
        local_dir = self.spark.conf.get("spark.local.dir")
        spill_before = dir_size(local_dir)

        with MemorySampler(self.jvm_pid) as mem, Timer() as timer:
            # days=None asks LakeReader for every partition of every source table, every
            # cycle. The two engines differ in more than this one argument -- indexes, the
            # silver feed, back-traversal, a checkpoint, a scoped upsert -- but this is the
            # line that decides the cost curve, and it is what the blog post is about.
            src = {
                name: latest_by_pk(self.reader.read_source(name), FACT_GRAPH.tables[name].pk)
                for name in FACT_GRAPH.tables
            }
            write_fact(build_fact(src), self.fact_dir)

        m.wall_s = timer.elapsed
        # Take the whole measurement, not just the headline number. Peak occupancy alone
        # ratchets upward with the JVM floor; the growth and provenance fields are what
        # make it readable as anything other than "memory went up".
        m.__dict__.update(mem.detail())
        # Clamped for the same reason as in incremental._finish: the ContextCleaner can
        # remove more than this cycle wrote, and a negative would be read as a saving.
        m.scratch_bytes = max(0, dir_size(local_dir) - spill_before)
        m.files_read = self.reader.files_read
        m.bytes_read = self.reader.bytes_read
        m.partitions_read = self.reader.partitions_read
        m.rows_written = parquet_row_count(self.fact_dir)
        # A full refresh rewrites everything, by definition. Stated explicitly so the
        # two engines' rows_rewritten mean the same thing when compared.
        m.rows_rewritten = m.rows_written
        m.fact_rows_total = m.rows_written
        m.fact_bytes_total = dir_size(self.fact_dir)
        m.notes = {"strategy": "full re-read + full join + full overwrite"}
        return m
