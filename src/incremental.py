"""The Razorpay technique: treat the Fact as an incrementally maintainable graph.

From the blog: "Instead of regenerating the full table, process only the change events
for each entity using the dependency graph to know which related rows to fetch, and a
secondary index to know where on the lake to find them."

One cycle, in the order the code runs it:

  1. index maintenance   append {join keys + created_date} for everything that changed
  2. primary flow        changed payments arrive from the silver layer already knowing
                         their own partition
  3. secondary flow      for EVERY dimension with genuine updates in this batch, walk the
                         graph backwards through the ancestor indexes to recover which
                         payments are affected and where they live. Which tables those are
                         is read off the graph and the CDC op code, not hardcoded: cards
                         are re-issued on every cycle, offers only on the scripted fan-out
                         ticks, and the longest walk (offers -> discounts_index ->
                         payments_index) is two hops.
  4. forward lookups     for every target payment, ask each index which partition holds
                         its order, its card, its discounts -- though only some of those
                         lookups earn their keep: discounts co-partition with their
                         payment, so that index is redundant by construction (see
                         generate.py), while cards cannot be pruned at all
  5. build               the same join the full refresh does, over a tiny slice
  6. scoped upsert       rewrite only the fact partitions that actually changed

Step 4 is the part that is easy to fake and would invalidate the whole demo. A payment's
card was created on a different day than the payment, so the payment's own partition says
nothing about where its card lives. Without cards_index you would scan the entire cards
table. The index join is a real, billed scan of a real index.

Where this loses, and it does lose:
  * every cycle pays index maintenance the full refresh never pays
  * the index scans in step 4 grow with total history, so the curve is flatter, not flat
  * step 6 rewrites whole partitions, so touching one row in a partition costs the
    whole partition
  * a changed dimension fans out across the entire history at once (step 3), and on the
    ticks where a hot offer changes this engine can cost more than the full refresh it
    replaced
"""

from __future__ import annotations

import glob
import json
import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .graph import FACT_GRAPH, fact_column_names
from .metrics import (
    MemorySampler,
    TickMetrics,
    Timer,
    dir_size,
    memory_limit_bytes,
    parquet_row_count,
)
from .pipeline import (
    FactPipeline,
    fact_partition_paths,
    latest_by_pk,
    read_fact_partitions,
    write_fact,
)
from .vanilla import build_fact


class IncrementalGraphRefresh(FactPipeline):
    name = "incremental"

    def __init__(self, spark, lake_dir: str, out_dir: str):
        super().__init__(spark, lake_dir, out_dir)
        self.index_dir = os.path.join(out_dir, "index")
        self.checkpoint_path = os.path.join(out_dir, "checkpoint.json")
        os.makedirs(self.index_dir, exist_ok=True)

    # --- secondary indexes --------------------------------------------------

    def _index_path(self, table: str) -> str:
        return os.path.join(self.index_dir, table)

    def _read_index(self, table: str) -> DataFrame | None:
        """Load a secondary index. This is a genuine scan of a genuine table.

        It is small -- a handful of narrow columns against a wide fact -- but it is not
        free, and it grows with history. This is the residual cost that keeps the
        incremental curve sloping gently upward instead of flat, and it is why the blog
        reports a ~10x win rather than an unbounded one.
        """
        paths = sorted(glob.glob(os.path.join(self._index_path(table), "*.parquet")))
        if not paths:
            return None
        self.reader.bill(paths, len(paths))
        return self.spark.read.parquet(*paths)

    def _maintain_indexes(self, ticks: list[int]) -> dict[str, DataFrame]:
        """Append these ticks' new keys to each index, and hand back the silver frames so
        the caller does not have to read (and pay for) the same files twice.

        Append-only is legitimate here because join keys are immutable in this dataset --
        a payment never changes which order it belongs to. A schema with mutable foreign
        keys would need a merge instead, and index maintenance would cost more. Note the
        index grows with update volume rather than entity count, since a row updated
        twenty times contributes twenty entries; `.distinct()` at probe time keeps the
        answers correct, and compaction is what a production version would add here.
        """
        silver = {}
        for table in FACT_GRAPH.tables:
            silver[table] = self.reader.read_silver(table, ticks)
        for table in FACT_GRAPH.indexed_tables():
            (silver[table].select(*FACT_GRAPH.index_columns(table))
                          .write.mode("append")
                          .parquet(self._index_path(table)))
        return silver

    # --- back-traversal -----------------------------------------------------

    def _payments_affected_by_dimension(self, table: str, changed: DataFrame) -> DataFrame | None:
        """Walk `table` back up to the root through the indexes, per the blog's secondary
        flow: "back-traverse the graph, joining with ancestor indexes to enrich with
        primary keys and partition values".

        For offers this is offers -> discounts_index (on offer_id) -> payment ids. The
        fact is never scanned to find out who is affected, which is the entire point;
        scanning it would cost the same as the full refresh.

        Returns (id, created_date): the blog specifies enriching "with the primary key
        AND partition values", and the last hop has the partition value in hand, so
        carrying it out avoids a second scan of the same index to recover it.
        """
        path = FACT_GRAPH.path_to_root(table)
        frontier = changed.select(F.col("id").alias("_k")).distinct()

        for i, edge in enumerate(path):
            parent_index = self._read_index(edge.parent)
            if parent_index is None:
                return None

            # `frontier` holds values of edge.child_col. Match them in the parent's index.
            #
            # The column carried forward is the one the NEXT hop will join on, not this
            # parent's primary key. Getting that wrong is subtle and silent: discount ids
            # and payment ids are both dense integers, so a mismatched hop still returns a
            # plausible number of plausible-looking ids, and only a row-level comparison
            # against the full refresh catches it.
            last = i + 1 == len(path)
            carry = ([F.col(FACT_GRAPH.tables[edge.parent].pk).alias("_k"),
                      F.col("created_date")] if last
                     else [F.col(path[i + 1].child_col).alias("_k")])

            # broadcast the frontier, never the index: the index is the side that grows
            # with history, so shuffling it on every hop would cost more than the fact scan
            # this traversal exists to avoid. The frontier is small on the first hop (one
            # changed offer) but NOT necessarily later -- it becomes the set of affected
            # keys, and on an offers fan-out cycle that is tens of thousands of payment
            # ids. Still the right side to broadcast; just not a trivial one.
            frontier = (parent_index
                        .join(F.broadcast(frontier),
                              F.col(edge.parent_col) == F.col("_k"), "inner")
                        .select(*carry)
                        .distinct())

        return frontier.withColumnRenamed("_k", "id")

    def _changed_dimensions(self, silver: dict[str, DataFrame]) -> list[tuple[str, DataFrame]]:
        """Dimension tables with genuine UPDATES in this batch, in level order.

        Driven by the graph rather than by a hardcoded table name. Inserts are skipped:
        a dimension row created in this batch belongs to a payment that is also in this
        batch, so the primary flow already rebuilds it and back-traversal would only
        rediscover rows it is about to process anyway. An UPDATE is the expensive case --
        the affected fact rows can be anywhere in history.
        """
        out = []
        for table in FACT_GRAPH.tables:
            if table == FACT_GRAPH.root:
                continue
            changed = silver[table].filter(F.col("op") == "u")
            if changed.head(1):
                out.append((table, changed))
        return out

    # --- forward lookups ----------------------------------------------------

    def _partitions_for(self, table: str, index_col: str,
                        targets: DataFrame, target_col: str) -> list[int]:
        """Which partitions of `table` hold the rows these targets point at?

        This is the lookup the whole technique rests on. The answer is a short list of
        partition numbers, and everything downstream reads only those.

        `targets` is broadcast and the index scanned, not the other way round: the index
        is the side that grows with history, so shuffling it once per probe would put back
        the cost the pruning is meant to remove. The hint is explicit rather than left to
        spark.sql.autoBroadcastJoinThreshold because which side is small is a property of
        the schema, not of whatever size estimate the optimiser arrives at this cycle.
        `.distinct()` first because many payments share a card, and duplicates would be
        collected to the driver and shipped to every task for no additional matches.

        The column to match on comes from the edge, not from the table's primary key.
        For payments->orders the index is probed by orders.id, but for the reverse edge
        payments.id = discounts.payment_id it must be probed by discounts.payment_id. Use
        the primary key blindly and the join still succeeds -- discount ids and payment
        ids are both dense integers over the same range -- it just silently returns the
        wrong partitions for a small minority of rows.
        """
        index = self._read_index(table)
        if index is None:
            return []
        hits = index.join(F.broadcast(targets.select(F.col(target_col).alias("_k")).distinct()),
                          F.col(index_col) == F.col("_k"), "inner")
        return [r[0] for r in hits.select("created_date").distinct().collect()]

    # --- the cycle ----------------------------------------------------------

    def run_tick(self, tick: int) -> TickMetrics:
        m = TickMetrics(tick=tick, engine=self.name, mem_limit_bytes=memory_limit_bytes())
        self._begin_tick(tick)
        local_dir = self.spark.conf.get("spark.local.dir")
        spill_before = dir_size(local_dir)
        cols = fact_column_names()

        # Ticks not yet folded into the fact. Normally just this one, but if a previous
        # cycle died the checkpoint is behind and this catches up -- the blog's first
        # complaint about the full refresh was that a failure meant restarting from
        # scratch because there was no safe checkpoint to resume from.
        pending = list(range(self._read_checkpoint() + 1, tick + 1))
        m.notes["pending_ticks"] = len(pending)

        with MemorySampler(self.jvm_pid) as mem, Timer() as timer:
            # 1. index maintenance -- a tax the full refresh never pays
            with Timer() as idx_timer:
                silver = self._maintain_indexes(pending)
            m.index_maint_s = idx_timer.elapsed

            # 2. primary flow: changed payments arrive as complete rows in the silver
            # layer, so their partitions never need to be opened at all. Only the
            # back-traversal targets below have to be fetched from the lake.
            # Drop the CDC op code once it has done its job: rows fetched from the lake
            # below do not carry it, and the fact build must see one schema.
            # Cached because it is counted, anti-joined once per changed dimension, and
            # then unioned into the build -- otherwise the window dedup runs every time.
            changed = latest_by_pk(silver["payments"], "id").drop("op").cache()
            m.changed_rows = changed.count()

            # 3. secondary flow: back-traversal from every dimension with real updates,
            # not just a hardcoded one. A changed dimension row arrives knowing nothing
            # about which fact rows it touches; the indexes are the only way to find out
            # without scanning the fact, which would cost what the full refresh costs.
            payments = changed
            # Every payment already scheduled for rebuild, by ANY route: the primary flow
            # plus every dimension processed so far this cycle. Two dimensions can reach
            # the same payment in one cycle -- a card re-issue and an offer change both
            # touching one row is unremarkable once the fact is large -- and each hit adds
            # a full row to `payments`, so a payment reached twice lands in the fact twice.
            #
            # Anti-joining only against `changed` catches the primary-flow collision and
            # misses the dimension-vs-dimension one entirely. That bug shipped, and it
            # reproduced at the default settings: a demo-scale sweep run hit a gate
            # MISMATCH on 2,450,000 rows. Every duplicated row held correct values, so
            # exceptAll could only report "extra rows on one side" -- which is why
            # verify.py now counts distinct ids and names duplication outright.
            # Diagnosing it needed the collision forced (card_updates raised 25 -> 6000),
            # which turned it into 118 duplicates out of 340,000 and made it reproducible
            # on demand; at stock settings it needs two dimensions to fan out in the same
            # cycle, so it surfaces rarely and looks like noise when it does.
            scheduled = changed.select("id")
            for table, changed_dim in self._changed_dimensions(silver):
                hits = self._payments_affected_by_dimension(table, changed_dim)
                if hits is None:
                    continue
                # Cached because it is counted, then collected, then joined.
                hits = hits.join(scheduled, "id", "left_anti").cache()
                n = hits.count()
                if not n:
                    continue
                scheduled = scheduled.unionByName(hits.select("id")).cache()
                m.fanout_rows += n
                m.notes.setdefault("fanout_by_table", {})[table] = n
                # created_date came out of the back-traversal, so no second index scan.
                fan_days = [r[0] for r in hits.select("created_date").distinct().collect()]
                # left_semi, not inner: those partitions also hold payments the changed
                # dimension does not touch, and semi keeps one row per surviving payment
                # while adding no columns. An inner join would both widen the frame and
                # duplicate rows for any id the back-traversal reported more than once.
                fan_rows = latest_by_pk(
                    self.reader.read_source("payments", fan_days)
                        .join(F.broadcast(hits.select("id")), "id", "left_semi"), "id")
                payments = payments.unionByName(fan_rows)

            if m.changed_rows + m.fanout_rows == 0:
                m.wall_s = timer.elapsed
                m.notes["strategy"] = "nothing changed"
                # Still advance: these ticks are accounted for, and leaving them pending
                # would make every later cycle replay them.
                self._write_checkpoint(tick)
                return self._finish(m, mem, local_dir, spill_before)

            # Cache before the forward lookups: `payments` is a union of the primary flow
            # and one fan-out branch per changed dimension, and every lookup below probes
            # it again. Uncached, each probe replays the whole union and its dedup.
            payments = payments.cache()

            # 4. forward lookups: walk the graph outward, asking each child's index which
            # partitions hold the rows these payments point at. Driven by the edges so the
            # probe columns can never drift out of sync with the join predicates.
            src = {"payments": payments}
            days_used = {}
            for edge in FACT_GRAPH.forward_order():
                child = edge.child
                if FACT_GRAPH.tables[child].partition_col is None:
                    # Small unpartitioned dimension: read it whole, no index needed.
                    src[child] = latest_by_pk(self.reader.read_source(child), "id")
                    continue
                parent_df = src[edge.parent]
                days = self._partitions_for(child, edge.child_col, parent_df, edge.parent_col)
                days_used[child] = len(days)
                src[child] = latest_by_pk(self.reader.read_source(child, days),
                                          FACT_GRAPH.tables[child].pk)

            # 5. same join as the full refresh, over a slice of the lake.
            # Cached because step 6 consumes it four times -- the distinct created_dates,
            # the anti-join against the existing partitions, the row count, and the write
            # -- and each one would otherwise re-run the whole five-table join.
            new_rows = build_fact(src).cache()

            # 6. upsert scoped to affected partitions
            affected = [r[0] for r in new_rows.select("created_date").distinct().collect()]
            existing_paths = fact_partition_paths(self.fact_dir, affected)
            existing = read_fact_partitions(self.spark, self.fact_dir, existing_paths, cols)
            m.notes["existing_files_read"] = len(existing_paths)
            if existing is not None:
                self.reader.bill(existing_paths, len(affected))
                # The delete half of the upsert: drop the versions being replaced, keep
                # everything else in the partition. Rewriting a partition means carrying
                # its untouched rows along -- touching one row costs the whole partition,
                # which is a real cost of this design and the reason partition width is a
                # tuning decision. new_rows is broadcast because it is the small side by
                # exactly the amplification factor being measured.
                kept = existing.join(F.broadcast(new_rows.select("id")), "id", "left_anti")
                out = kept.unionByName(new_rows)
            else:
                out = new_rows

            # Materialise BEFORE writing. `out` reads the very partitions write_fact is
            # about to replace, so leaving it lazy is a read-your-own-write hazard: the
            # plan would be re-evaluated against files that no longer exist in the form it
            # was built from. Counting here forces the cache to fill first, which both
            # fixes the hazard and gives an honest number.
            out = out.cache()
            m.rows_written = new_rows.count()
            # Rows physically rewritten, including untouched ones dragged along because
            # their partition had to be replaced. The gap between this and rows_written
            # IS the partition-rewrite amplification -- the cost of partition-granular
            # updates, and the number that tells you whether your partitions are too wide.
            m.rows_rewritten = out.count()
            write_fact(out, self.fact_dir)
            m.notes.update({
                "strategy": "index-guided incremental + partition-scoped upsert",
                "affected_partitions": len(affected),
                "source_partitions_read": days_used,
                "write_amplification": round(m.rows_rewritten / max(m.rows_written, 1), 2),
            })
            out.unpersist()
            new_rows.unpersist()
            payments.unpersist()
            changed.unpersist()

        m.wall_s = timer.elapsed
        self._write_checkpoint(tick)
        return self._finish(m, mem, local_dir, spill_before)

    # --- bookkeeping --------------------------------------------------------

    def _finish(self, m: TickMetrics, mem, local_dir: str, spill_before: int) -> TickMetrics:
        # Take the whole measurement, not just the headline number. Peak occupancy alone
        # ratchets upward with the JVM floor; the growth and provenance fields are what
        # make it readable as anything other than "memory went up".
        m.__dict__.update(mem.detail())
        # Clamped: Spark's ContextCleaner deletes shuffle files on its own schedule, so a
        # cycle that cleans up more than it writes gives a negative delta. That is an
        # artefact of when the cleaner ran, not local-disk pressure, and reporting it as
        # a negative would be worse than reporting zero.
        m.scratch_bytes = max(0, dir_size(local_dir) - spill_before)
        m.files_read = self.reader.files_read
        m.bytes_read = self.reader.bytes_read
        m.partitions_read = self.reader.partitions_read
        m.fact_rows_total = parquet_row_count(self.fact_dir)
        m.fact_bytes_total = dir_size(self.fact_dir)
        m.notes["index_bytes"] = dir_size(self.index_dir)
        return m

    def _read_checkpoint(self) -> int:
        """Last tick successfully folded into the fact, or -1 for a cold start.

        Written only after the upsert succeeds, so a cycle that dies leaves the
        checkpoint behind and the next run replays from there instead of rebuilding
        everything. This is the property the blog opens with: the full refresh "required
        the full dataset at once, there was no safe checkpoint -- a single failure meant
        restarting from scratch".
        """
        try:
            with open(self.checkpoint_path) as f:
                return int(json.load(f)["last_tick"])
        except (OSError, ValueError, KeyError):
            return -1

    def _write_checkpoint(self, tick: int) -> None:
        with open(self.checkpoint_path, "w") as f:
            json.dump({"last_tick": tick}, f)
