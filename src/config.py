"""Shared configuration. Deliberately boring: constants and dataclasses, no framework.

Everything both engines could possibly disagree on lives here, so neither can get an
unfair edge. See spark_conf() in pipeline.py for the settings that are asserted identical
across the two runs.

The lake layout itself is NOT declared here -- the set of tables, their primary keys and
which of them are partitioned all live in graph.py, because the graph is what the
incremental engine traverses and a second copy of that list would be free to drift.
What matters about the layout, and is assumed everywhere below: sources are partitioned
by created_date (when the entity was born), which is what makes back-dated updates
expensive. A payment updated today may live in a partition from months ago, and without
an index you cannot know which one.
"""

from __future__ import annotations

from dataclasses import dataclass

# --- churn model ------------------------------------------------------------
# The single most consequential modelling choice in this demo, so it is a switch rather
# than a buried constant. It decides whether the technique wins at all.
#
#   "recent"   A payment is created, moves through its status transitions over the next
#              few cycles, and is then rarely touched again -- with a genuine long tail of
#              back-dated refunds and disputes reaching all the way through history.
#              Change volume per cycle is roughly CONSTANT while the table keeps growing.
#              This is what a payments ledger actually does, and it is the regime in which
#              incremental denormalisation is the right answer.
#
#   "uniform"  A fixed percentage of the ENTIRE table is rewritten every cycle, drawn
#              uniformly across all history, so change volume grows with the table and
#              every partition is dirty every cycle. The scoped upsert degenerates into a
#              full rewrite and the advantage collapses -- the engine still pays for its
#              indexes and stops getting anything back. Not a strawman: it is the honest
#              adversarial case, and running it is how you find out whether your workload
#              is one the technique can help.
#
# The demo ships "recent" as the default and keeps "uniform" one flag away, because
# "when does this stop working" is as much the point as "how much does it win by".

CHURN_MODELS = ("recent", "uniform")

# Only used by the "uniform" model: share of the whole table rewritten per cycle.
UNIFORM_UPDATE_FRAC = 0.02


# --- SLA: how long one cycle is allowed to take -----------------------------
# Each tick is ONE SIMULATED DAY of data (see _tick_days in generate.py), so the deadline
# a daily pipeline actually has is a day: finish before tomorrow's data lands. The demo
# compresses that day into wall clock, and the compression factor has to be derived from
# something or the freshness-lag chart is just a tuned constant.
#
# The anchor is the situation the blog describes: the full refresh WAS meeting its
# schedule, right up until the table outgrew it. So one simulated day is worth exactly
# what a full refresh of the DAY-ZERO table costs -- the schedule was set on deployment
# day and it fitted then, and everything after that is growth it never budgeted for.
#
# That cost is a property of the box, not of the demo, so it is two constants rather than
# one magic number: a fixed per-cycle floor (JVM job launch plus the full-history dedup,
# neither of which shrinks with the table) and a per-row term. Fitted to tick-0 vanilla
# wall time on the reference box -- a 2 vCPU / 3 GB container, the compose default:
#
#     smoke    200k rows -> 7.0s   (measured 6.7s and 9.2s on two runs)
#     demo       2M rows -> 16.0s  (measured 16.3s on three runs)
#     stress     6M rows -> 36.0s  (extrapolated from the fit, not measured)
#
# A faster or slower box moves both constants. Re-measure tick 0 of the vanilla engine and
# adjust them, or override the slot for a single run with `run_engine.py --tick-interval`,
# which prints the value it is using.
REF_CYCLE_FIXED_S = 6.0
REF_CYCLE_S_PER_M_ROWS = 5.0


@dataclass(frozen=True)
class Scale:
    """One preset.

    The shape matters more than the numbers. Razorpay's situation is not "a table that
    grows from nothing" -- it is a LARGE EXISTING Fact with multi-year retention, against
    which each cycle applies a small, mostly recent set of changes. A demo that starts
    empty and grows uniformly models a different problem and gets a different answer, so
    every preset here starts from a backfill and then applies daily deltas.
    """

    name: str
    # The Fact that already exists on day zero, and how many daily partitions it spans.
    backfill_payments: int
    backfill_days: int
    # Daily cycles to run after the backfill. Each adds one new partition.
    ticks: int
    new_per_tick: int
    # How long a payment keeps receiving updates. Sized in PARTITION LOCALITY below,
    # which is also where the reason it matters lives.
    recent_window: int
    recent_updates_per_new: float
    # ...plus a thin tail of genuinely back-dated corrections (chargebacks, disputes, late
    # reconciliation) drawn uniformly from the ENTIRE history. A FRACTION OF DAILY VOLUME
    # rather than an absolute count, because a rate scales with the business and a count
    # does not: a fixed 100 corrections/day against a growing ledger quietly means the
    # correction rate falls as the table grows, which is backwards and would flatter the
    # incremental engine at exactly the scales it is meant to be tested at.
    #
    # The single most consequential knob in the demo -- see PARTITION LOCALITY.
    tail_update_rate: float
    # Ticks on which a single hot `offers` row is updated, fanning out across history.
    # This is the case that makes incremental *worse*, and it is the reason Razorpay
    # leaves high-cardinality dimensions to runtime joins instead of denormalising them.
    fanout_ticks: tuple[int, ...]
    # Cards re-issued / corrected per cycle. Small but nonzero on EVERY cycle, so the
    # generic secondary flow is exercised continuously rather than only on the two
    # scripted offer ticks. This matters for correctness, not just speed: an engine that
    # mishandled a dimension update would leave stale fact rows, and without real
    # dimension updates in the data the correctness gate would never notice.
    card_updates: int = 25

    @property
    def total_days(self) -> int:
        return self.backfill_days + self.ticks

    @property
    def sla_slot_s(self) -> float:
        """Wall clock representing one simulated day at this scale. See SLA above.

        Sized on the day-zero table, not the current one: the schedule does not grow just
        because the Fact did.
        """
        return REF_CYCLE_FIXED_S + REF_CYCLE_S_PER_M_ROWS * self.backfill_payments / 1e6


# PARTITION LOCALITY -- the thing that decides whether any of this pays off.
#
# A partition-scoped upsert only saves work if the changed rows sit in a minority of
# partitions, and TWO parameters together decide that:
#
#   recent_window     how long a payment stays active. Updates inside the window land in
#                     a handful of ADJACENT partitions, so they are nearly free. 30 days
#                     covers the real lifecycle: authorise, capture, settle, refund, and
#                     the bulk of the chargeback window.
#   tail_update_rate  corrections arriving AFTER that window. These are what hurt: N
#                     scattered corrections can dirty up to N different partitions, one
#                     row at a time, and each one forces a whole partition rewrite.
#
# At the shipped 0.15% of daily volume this churn alone dirties 20-35% of partitions per
# cycle depending on the preset, and the technique wins clearly. Push the rate to ~1% and
# 70-90% of them are dirty, the upsert degenerates into a full rewrite, and the incremental
# engine does strictly more work than the full refresh it replaced -- it still pays for
# indexes, it just stops getting anything back.
#
# Those figures count the partitions of changed PAYMENTS only. What the engine actually
# rewrites is higher -- a measured median of ~37% at demo -- because a changed DIMENSION
# dirties partitions too, and card re-issues land anywhere in history. Compare
# affected_partitions in the results against this number; the gap is the fan-out.
#
# That is not a footnote, it is the adoption criterion. Measure this rate on your own data
# before believing any of the numbers here; `python -m src.sweep` walks it for you, and
# --churn uniform is the same failure without the sweep.

SCALES = {
    # Fast enough to run the correctness gate on every change.
    "smoke": Scale("smoke", backfill_payments=200_000, backfill_days=120, ticks=8,
                   new_per_tick=20_000, recent_window=30, recent_updates_per_new=1.0,
                   tail_update_rate=0.0015, fanout_ticks=(5,)),
    # The default: a year of history, three weeks of daily cycles.
    "demo": Scale("demo", backfill_payments=2_000_000, backfill_days=365, ticks=20,
                  new_per_tick=50_000, recent_window=30, recent_updates_per_new=1.0,
                  tail_update_rate=0.0015, fanout_ticks=(8, 15)),
    # 3x the demo backfill, sized to push a full refresh into the memory cap rather than
    # merely slow it down. Whether it actually gets there depends on the box.
    #
    # UNVALIDATED. Nothing in this repo has run this preset end to end: no recorded run
    # carries scale="stress", no run of any preset has ever produced an oom or error tick,
    # and this preset's SLA slot (36s) is extrapolated from the smoke and demo fits rather
    # than measured. Treat "the full refresh breaks here" as a hypothesis to test, not a
    # result to cite -- and if you do run it, the honest first check is whether the cap is
    # reached at all before the run simply takes an hour.
    "stress": Scale("stress", backfill_payments=6_000_000, backfill_days=365, ticks=20,
                    new_per_tick=100_000, recent_window=30, recent_updates_per_new=1.0,
                    tail_update_rate=0.0015, fanout_ticks=(8, 15)),
}

# --- entity cardinalities ---------------------------------------------------
# Orders are 1:1 with payments. Cards and offers are shared dimensions, so a
# single card or offer row is referenced by many payments -- that sharing is what
# creates the fan-out.
CARDS_PER_TICK_RATIO = 0.30      # 30% of new payments introduce a new card
DISCOUNT_ATTACH_RATE = 0.40      # 40% of payments carry a discount
N_OFFERS = 50                    # small dimension, heavily shared
N_MERCHANTS = 5_000

# --- determinism ------------------------------------------------------------
SEED = 20260727

# --- paths (inside the container; overridable for host-side dev) -------------
# Lake and output directories are passed in per service by docker-compose, since the two
# engines deliberately write to different ones. Only the shared results directory, which
# both engines and the host-side dashboard agree on, is pinned here.
RESULTS_DIR = "/results"
