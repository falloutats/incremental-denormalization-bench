# Full refresh vs. incremental denormalization — a minimal, honest demo

Two pipelines build the **same** denormalized reporting table from the same lake, under
the same CPU and memory caps, in separate containers. One rebuilds everything every
cycle. The other maintains it incrementally using a dependency graph and secondary
indexes, the way Razorpay describes in
[How We Refresh Razorpay's Data Warehouse 10x Faster with Graphs and Indexes](https://engineering.razorpay.com/how-we-refresh-razorpays-data-warehouse-10x-faster-with-graphs-and-indexes-538abc244703)
(the companion to the Fifth Elephant 2026 talk *Full refresh to incremental: rebuilding
denormalization for reporting at Razorpay scale*).

The point is not to show that incremental wins. It is to show **where the full refresh
starts to break, how much the incremental approach costs you to adopt, and the specific
conditions under which it stops being worth it.**

---

## Start here

**Run it first, read second.** The smallest preset gives you real output to read the rest
against:

```bash
./scripts/install-brew.sh && ./scripts/setup-python.sh   # once
./run.sh --scale smoke                                   # ~3 min, +2 min image build first time
```

At `smoke` the incremental engine **loses on every cycle**. That is the correct result at
that size, not a broken run — see [It loses outright at small scale](#it-loses-outright-at-small-scale).
Use `./run.sh` (the `demo` preset, ~15 min) to see it win.

Then pick the path that matches why you are here. Each is 10–15 minutes; none require
the others.

| You want to… | Read, in order |
|---|---|
| **See the result and decide if it applies to you** | [What it actually measures](#what-it-actually-measures) → [Why 1.15x and not the blog's 10x](#why-115x-and-not-the-blogs-10x) → [The modelling choice that decides the whole result](#the-modelling-choice-that-decides-the-whole-result) |
| **Understand the technique** | [What the two engines do](#what-the-two-engines-do) → `src/graph.py` → `src/incremental.py` → `src/vanilla.py` |
| **Check that it isn't rigged** | [The rules that keep it honest](#the-rules-that-keep-it-honest) → [Known asymmetries](#known-asymmetries-that-still-favour-incremental) → `src/verify.py` → `src/pipeline.py` (`LakeReader.bill`) |
| **Decide whether to adopt this at work** | [Where the incremental engine is worse](#where-the-incremental-engine-is-worse) → [The modelling choice…](#the-modelling-choice-that-decides-the-whole-result) → run `python -m src.sweep` on your own churn rate |

### The three-minute version

If you read nothing else, read these four things in this order:

1. **`src/vanilla.py`** (~40 lines) — the baseline. Read all of it. Everything else exists
   to be compared against this.
2. **`src/graph.py`** — tables as nodes, join predicates as edges. `path_to_root()` is the
   whole trick: how a changed dimension row finds the fact rows it affects *without*
   scanning the fact.
3. **`src/incremental.py`** — `run_tick()` reads top-to-bottom as the six numbered steps
   in [What the two engines do](#what-the-two-engines-do). Skip the helpers on a first pass.
4. **[Where the incremental engine is worse](#where-the-incremental-engine-is-worse)** — the
   part that makes this a demo rather than an advertisement.

Only two files are allowed to be complex, and they are #2 and #3. Everything else is
deliberately boring; if a file surprises you, that is a bug.

### Where the numbers come from

`src/generate.py` → the lake. `src/run_engine.py` → one engine over all cycles, one JSONL
line per cycle. `dashboard/report.py` → the tables and charts. `src/metrics.py` defines
every field; when a number looks odd, that module's docstring says what it actually means.

---

## What it actually measures

`--scale demo` (2M-row backfill over a year of daily partitions, 20 daily cycles,
2 CPU / 3 GB per container), correctness gate passing on 2,950,000 identical rows:

Every figure below is reproduced from the committed `results/` — run
`python -m dashboard.report --results ./results` to regenerate the table yourself.

| | full refresh | incremental |
|---|---|---|
| typical cycle (median) | 16.3s | **14.3s — 1.15x** |
| per-cycle ratio, range | — | **0.62x … 1.34x** (it loses on some cycles) |
| cumulative over 20 cycles | 362s | **343s — 1.1x** |
| bytes read per cycle | 160.9MB | **170.9MB — it reads MORE, 0.94x** |
| partition-rewrite amplification | 1x | **12x median, 14.4x by the last cycle** |
| fan-out cycles (8, 15) | 16.1s / 20.1s | **25.0s / 32.3s — loses outright** |
| freshness lag after 20 cycles | 51s (3.2 days) | **34s (2.1 days)** |

**Read the bytes row again.** The incremental engine reads *more* bytes than the full
refresh. Its entire advantage comes from joining and rewriting less, not from reading less
— two of its three dimensions cannot be pruned (see "where it is worse", #5), and
rewriting 12x more rows than it changed eats most of what is left. A 1.15x median with a
worst cycle at 0.62x is the honest shape of this technique at this scale.

These numbers move between runs — an earlier run on a busier machine measured the full
refresh at 20.1s and the ratio at 1.34x. The *shape* is stable (flat-ish incremental, full
refresh scaling with the table, fan-out cycles losing); the exact ratio is not. Treat one
run as one sample.

### Why 1.15x and not the blog's 10x

Worth stating plainly rather than quietly hoping nobody asks:

- **This fact joins 5 tables; Razorpay's join 10–30.** The full refresh's cost scales with
  table count — it re-reads and re-joins *every* one of them every cycle — while the
  incremental engine reads a pruned slice of each. That gap is where most of the missing
  factor lives, and a 5-table demo structurally cannot show it.
- **`cards` is denormalized here, and it should not be.** It is a high-cardinality
  dimension referenced uniformly across all history, so no index can prune it and every
  re-issue rewrites fact partitions everywhere. Razorpay leave exactly these as runtime
  bucketed joins. This demo denormalizes it on purpose so you can *see* the cost.
- No Delta/Iceberg MERGE, no bucketing, no index compaction. All of which they have.

The honest summary is that this reproduces the *mechanism* and its *tradeoffs* faithfully,
at a scale and shape where the payoff is ~1.15x. Do not read the ratio as a prediction for
your own system; read the diagnostics as a method for predicting it.

---

## What the two engines do

Both are built from the same `FactPipeline` base, get an identical Spark config
(asserted by fingerprint at startup), read through the same `LakeReader`, and use the
same `build_fact()` join. The only thing either one controls is **which files it asks
for**.

### `src/vanilla.py` — the baseline (~40 lines)

```
read ALL partitions of ALL source tables   →   full join   →   overwrite the whole fact
```

This is not a strawman. It is the obvious, correct implementation that most teams ship
and run happily for a year. Adaptive execution, broadcast joins and vectorized reads are
all left on. Its single property is that **its cost tracks how much data exists, not how
much changed.**

### `src/incremental.py` — the technique

```
1. index maintenance   append {join keys + created_date} for whatever changed
2. primary flow        changed payments arrive from the silver layer knowing their partition
3. secondary flow      every dimension with real updates knows nothing about which fact
                       rows it touches — back-traverse the graph through the ancestor
                       indexes (offers → discounts_index → payments_index) to find them
4. forward lookups     ask each child's index which partitions hold its related rows
5. build               the same join, over a slice
6. scoped upsert       rewrite only the fact partitions that actually changed
```

The fact is modelled as a directed graph (`src/graph.py`) — nodes are tables, edges are
join predicates:

```
payments ──order_id=orders.id──────▶ orders
         ──card_id=cards.id───────▶ cards
         ──id=discounts.payment_id▶ discounts ──offer_id=offers.id──▶ offers
```

Five tables rather than Razorpay's 10–30, but with a real 2-hop edge, because
back-traversal across more than one hop is where the technique gets hard and where its
costs live.

---

## The rules that keep it honest

A demo like this is trivially riggable. These are the specific ways it could cheat, and
what stops each one:

| Cheat | What prevents it |
|---|---|
| Nerf the baseline | Same Spark conf (fingerprint compared across both runs), same parquet layout, same dedup helper, same join code. Nothing disabled. |
| Give incremental a free partition index | The `silver/` feed carries changed rows, but a payment's own partition tells you nothing about where its *card* or *order* lives — those are deliberately decorrelated. Indexes are built and maintained by the engine and every second is billed to it. |
| Fake partition pruning | `LakeReader` reads by explicit file path and sums the real bytes of every file opened. There is no way to read a file without it showing on the bill. |
| Only update recent rows | Every cycle includes a tail of genuinely back-dated updates drawn uniformly from the whole history, forcing real index lookups into year-old partitions. |
| Stub the hard part | The 2-hop back-traversal is implemented, and the secondary flow is driven by the graph rather than hardcoded to one table. |
| Hide the merge cost | `rows_rewritten` vs `rows_written` is reported every cycle. Both engines define it the same way. |
| Win by doing less work | **The correctness gate.** Both fact tables must be row-for-row identical or the run is declared void. |

That gate is not decoration. It caught two real bugs during development, both of which
produced *plausible* output: payment ids and discount ids are both dense integers over
overlapping ranges, so a mis-specified join hop still returns a believable number of
believable-looking rows. Only a row-level comparison against the full refresh found them.
Razorpay describe doing record-by-record reconciliation and shadow A/B testing before
letting this serve merchants; this is the same discipline at 1/1000th the scale.

### Known asymmetries that still favour incremental

Two auditors went at this specifically looking for rigging. These survived, and you should
weigh them when reading the numbers:

- **The full refresh cannot use the silver layer.** Silver holds per-tick deltas, so only
  an incremental consumer can use it; the baseline re-derives current state from raw
  append-only files every cycle (a full window dedup over the whole table, forever). A
  shop that already has the silver layer the blog describes would have a *deduplicated
  current-state table* its full refresh could read, and would skip that work. Fixing this
  would narrow the gap.
- **Partition granularity is favourable.** ~5,500 rows per daily partition is finer than a
  real lake would leave uncompacted, and finer partitions reduce exactly the rewrite
  amplification that is the technique's main cost.
- **Writes are not billed.** Only `bytes_read` is charged. The full refresh rewrites 100%
  of the fact every cycle; counting write bytes would make incremental look *better*.
  Listed for completeness — this one runs against incremental.

**An audit found and fixed a demo-invalidating flaw.** The first version made `orders`
1:1 with payments *sharing the same `created_date`*, which meant the orders index returned
exactly what the changed payment's own partition already said — the index was decoration
and the read-amplification number was inflated by roughly 30%. Orders are now back-dated
relative to their payment, which is the case the blog explicitly names ("payments
referencing old orders … full table scans on every secondary table"). The measured
advantage dropped when this was fixed. That is the correct direction for a fix to move a
number.

---

## The modelling choice that decides the whole result

**How partition-local is your churn?** This matters more than anything about the code,
so it is a flag rather than a buried constant (`CHURN_MODELS` in `src/config.py`).

The presets model Razorpay's actual situation: a **large existing fact** with a year of
daily partitions, against which each cycle applies **a small, mostly recent set of
changes** — plus a thin tail of back-dated corrections.

- `--churn recent` (default). Change volume per cycle is roughly constant while the table
  keeps growing. A partition-scoped upsert dirties **~30–40% of partitions per cycle**
  at the demo preset (median 37% measured; 100% on the two dimension fan-out cycles).
  About 23 points of that is the back-dated payment churn itself; the rest is dimension
  fan-out — 25 card re-issues a cycle, each reaching payments scattered anywhere in
  history. This is the regime where the technique is the right answer, but note that even
  here it rewrites a third of the table to change a fraction of a percent of it.
- `--churn uniform`. A fixed share of the *entire* table is rewritten every cycle, drawn
  uniformly. Every partition is dirty every cycle, the scoped upsert degenerates into a
  full rewrite, and the incremental engine does strictly more work than the full refresh
  it replaced — it still pays for indexes, it just stops getting anything back.

`tail_update_rate` in each `Scale` is the direct control: it is a fraction of daily
volume, and *N* back-dated corrections can dirty at most *N* partitions. Run both, or
sweep the rate with `python -m src.sweep`, and the adoption criterion becomes obvious.

---

## Where the incremental engine is worse

Reported in its own table at the end of every run, not buried:

1. **Index maintenance** — paid every cycle, forever, by the incremental engine only.
2. **Cold start** — on a small table it is straightforwardly slower. There is a crossover
   and the live dashboard's verdict panel prints where it is; the report prints the
   first-cycles comparison it comes from.
3. **Partition-rewrite amplification** — measured, not asserted. Touching one row costs
   its whole partition, so the engine rewrites several rows for every row that actually
   changed, and the ratio grows as history accumulates. Partition width becomes a tuning
   decision you did not previously have.
4. **Dimension fan-out** — on scripted ticks a single `offers` row changes. Back-traversal
   finds thousands of affected payments scattered across the entire history, amplification
   spikes several-fold, and that cycle costs **more than the full refresh**. This is
   exactly why Razorpay leave high-cardinality dimensions as runtime bucketed joins
   instead of denormalizing them.
5. **Dimensions that refuse to prune** — the three dimensions here deliberately cover the
   three regimes you actually meet, and the report prints partitions-read for each:
   - `discounts` co-partition with their payment → the index is *redundant*; the parent's
     own partition already answers the lookup.
   - `orders` are back-dated with a long tail → the index prunes, and earns its keep.
   - `cards` are referenced uniformly across all history → **no index can narrow this**,
     and it scans every partition every cycle. That column should be a runtime join.

   Running this diagnostic on your own data is arguably the most useful thing here.
6. **Higher peak memory** — measured, and it surprised me: the incremental engine's peak
   occupancy sits *above* the full refresh's, because it caches intermediate frames and
   runs many more small Spark jobs per cycle. The full refresh streams; it does not have
   to hold much at once. Read the size of the gap off `mem_peak_growth_bytes`, not off
   `peak_rss_bytes`: peak occupancy includes the long-lived JVM floor and therefore
   ratchets upward across cycles whatever either engine does (`src/metrics.py`).
7. **Complexity** — three moving parts (silver feed, indexes, checkpointed merge) instead
   of one, plus a reconciliation harness before you can trust any of it.

### It loses outright at small scale

At `--scale smoke` (340k rows) the incremental engine is **0.58x — slower on every single
cycle** (per-cycle ratios 0.37x to 0.77x), and cumulatively 79s vs 44s. Those figures come
from `results-smoke/`, committed alongside the demo run so both are checkable. That is not a bug, and it is not tuned away: index
maintenance, ~5x rewrite amplification, and dimension fan-out all cost the same whether
the table is small or large, while the full refresh's cost is simply proportional to a
table that is not yet big enough to hurt.

The crossover is real and the demo shows both sides of it. Run `smoke` before `demo` and
watch the sign flip.

### Known simplifications

Stated plainly so the numbers are not over-read. Both of these make the incremental
engine look **worse** than the blog's design, not better:

- The secondary flow rebuilds affected fact rows in full, where the blog upserts only the
  changed dimension's columns. This inflates the fan-out spike in item 4.
- Indexes are append-only, so they grow with update volume rather than entity count. The
  blog upserts them. Production would compact; this does not.

Not built at all: Delta/Iceberg MERGE, `merchant_id` bucketing, runtime joins for
high-cardinality dimensions, streaming/Flink, self-healing checkpoint fallback, data
quality alerting.

---

## Reading the output

The live dashboard (`dashboard/live.py`) runs **on the host, outside both containers**, so
it never competes for the budget being measured. Four charts:

- **cycle time vs table size** — the headline. The full refresh climbs with total rows;
  the incremental engine tracks change volume. The crossover tick is named in the verdict
  panel under the charts.
- **read amplification** — the *why* behind the first chart, in bytes actually opened.
- **peak memory vs the hard cap** — how close each container is running to the ceiling
  docker enforces. It is occupancy, not per-cycle demand: the curve ratchets because the
  JVM never returns heap. If a cycle is OOM-killed the tick and row count are recorded.
- **freshness lag** — cycles that overrun their slot compound into SLA debt. The slot is
  per-preset (`Scale.sla_slot_s`, sized on the day-zero refresh), so it is a real deadline
  rather than a tuned constant. This is the mechanism behind the 48-hour staleness
  described in the blog.

---

## Controlling the box

```bash
./run.sh --scale stress          # the largest preset: 6M-row backfill under the same cap
./run.sh --mem 2g --cpus 2       # shrink the box; the breaking point moves earlier
./run.sh --churn uniform         # the regime where incremental stops winning
./run.sh --parallel              # both engines at once, for the side-by-side race
```

Both containers get identical `cpus`, `mem_limit` and `memswap_limit` (no swap escape
hatch), and identical `spark.driver.memory` and shuffle settings. Sequential is the
default because it is the mode whose timings can be trusted: the containers are separately
capped but still share one disk. `--parallel` races them, which reads better on a live
dashboard, at the cost of I/O contention neither container's cap accounts for.

Raising `mem_limit` by 2× should move the full refresh's breaking point later without
removing the divergence — that is the check that the result is algorithmic rather than a
tuning artifact.

---

## Layout

Read in roughly this order; the two starred files are the only ones allowed to be complex.

```
run.sh                    the entry point: build → generate → both engines → gate → report
scripts/install-brew.sh   system deps (OrbStack, Java 17) — brew only
scripts/setup-python.sh   host venv for the dashboard — uv only
env.sh                    JAVA_HOME, SPARK_LOCAL_IP, OrbStack on PATH

src/vanilla.py            the baseline — start here, it is ~40 lines
src/graph.py            * the fact dependency graph: forward and backward traversal
src/incremental.py      * the technique
src/verify.py             the correctness gate that makes every number above mean something

src/config.py             scale presets, churn models, the knobs that decide the result
src/generate.py           deterministic backfill + daily deltas (numpy/pyarrow, no Spark)
src/pipeline.py           shared Spark conf, billed LakeReader, shared fact I/O
src/metrics.py            what every reported number actually means — read before doubting one
src/run_engine.py         runs one engine over all cycles, one JSONL line per cycle
src/run_verify.py         container entrypoint for the gate
src/sweep.py              churn-rate sensitivity sweep: find your own break-even point

dashboard/common.py       the engine list, colours and byte formatting both share
dashboard/live.py         live terminal dashboard (host-side, runs outside the caps)
dashboard/report.py       final summary, curves, and the where-it-loses table
```
