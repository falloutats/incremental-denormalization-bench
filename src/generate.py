"""Deterministic tick generator. Plain numpy + pyarrow, no Spark -- it is not part of
what we are measuring, and keeping it out of Spark keeps the measured runs clean.

Lake layout produced here:

  source/<table>/created_date=<D>/t<NNNN>_<kind>.parquet
      Append-only history, `kind` being "i" for the batch of inserts a tick produced and
      "u" for its updates. A row appears once when created and again every time it is
      updated; readers dedupe by primary key keeping max(updated_at). This is what an
      uncompacted lake actually looks like, and both engines read the same bytes. The
      tick number in the filename is what lets a reader reconstruct the lake as it stood
      at any earlier tick without regenerating anything.
      (Unpartitioned dimensions -- offers -- drop the created_date directory and sit
      directly under source/<table>/.)

  silver/<table>/tick=<NNNN>/part[_<label>].parquet
      The blog's "deduplicated silver layer partitioned by updated_date": the full rows
      that changed in that tick. One file per write, not one per tick, because a table
      can change more than once in a cycle (new cards AND re-issued cards) and a fixed
      filename would silently drop whichever batch landed first. This is landed CDC,
      shared upstream infrastructure that any pipeline would have. The incremental engine
      is billed for every byte it reads out of it; the full refresh ignores it, because
      current-state tables are all it needs.

What the silver layer deliberately does NOT hand over is a shortcut to where related rows
live. A changed payment row knows its own partition, and that is all: cards are created
on days unrelated to the payments that use them (see card_created below) and orders are
back-dated relative to their payment, so the SET of partitions holding them cannot be
derived from the changed payments' own created_dates. (Most orders do land on their
payment's day; it is the minority that do not which makes the lookup necessary, because
missing them produces a wrong fact, not merely a slow one.) A changed offer arrives
knowing nothing at all. Those lookups are what the secondary indexes exist for, and they
stay expensive.

The one exception is deliberate and is reported rather than hidden: discounts are raised
with their payment and therefore genuinely co-partition with it, so for that dimension the
index is redundant. Keeping all three regimes -- redundant (discounts), load-bearing
(orders), unprunable (cards) -- is the point, and the per-dimension partitions-read
counts in the report are what you read each regime off. Silently making every dimension
co-partition is the easiest way to accidentally build a demo that proves nothing.

The second load-bearing choice is in _sample_updates: how much of history churns per
cycle, and how it is distributed. See CHURN_MODELS in config.py -- it decides the result,
so it is a flag rather than a buried assumption.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .config import (
    CARDS_PER_TICK_RATIO,
    CHURN_MODELS,
    DISCOUNT_ATTACH_RATE,
    N_MERCHANTS,
    N_OFFERS,
    SCALES,
    SEED,
    UNIFORM_UPDATE_FRAC,
    Scale,
)

STATUSES = np.array(["created", "authorized", "captured", "refunded", "failed"])
NETWORKS = np.array(["visa", "mastercard", "rupay", "amex"])


class LakeWriter:
    """Writes partitioned parquet. Boring on purpose."""

    def __init__(self, root: str):
        self.root = root
        self.files = 0

    def write_partitioned(self, table: str, tick: int, arrays: dict, created_date: np.ndarray,
                          kind: str = "i"):
        """One file per distinct created_date touched by this batch.

        Filenames carry the tick that produced them (t0007_i / t0007_u) so a reader can
        reconstruct the lake as it stood at any point in time. The whole run is generated
        up front for determinism, but an engine at tick N must only see files from ticks
        <= N, or it would be joining data that has not arrived yet.
        """
        if len(created_date) == 0:
            return
        # Sort-then-split rather than a loop over np.unique(created_date): one pass over
        # the batch instead of one full mask per day, which matters at 6M backfill rows.
        # `stable` so rows sharing a created_date keep their generated order within a
        # file. Determinism does not depend on it -- argsort is deterministic for a fixed
        # input either way -- it just makes a diff between two lakes readable, and
        # "both engines replay identical input" stays checkable rather than assumed.
        order = np.argsort(created_date, kind="stable")
        sorted_dates = created_date[order]
        bounds = np.flatnonzero(np.diff(sorted_dates)) + 1
        for chunk in np.split(order, bounds):
            day = int(created_date[chunk[0]])
            path = os.path.join(self.root, "source", table, f"created_date={day}")
            os.makedirs(path, exist_ok=True)
            tbl = pa.table({k: pa.array(v[chunk]) for k, v in arrays.items()})
            pq.write_table(tbl, os.path.join(path, f"t{tick:04d}_{kind}.parquet"),
                           compression="snappy")
            self.files += 1

    def write_unpartitioned(self, table: str, tick: int, arrays: dict, kind: str = "i"):
        path = os.path.join(self.root, "source", table)
        os.makedirs(path, exist_ok=True)
        tbl = pa.table({k: pa.array(v) for k, v in arrays.items()})
        pq.write_table(tbl, os.path.join(path, f"t{tick:04d}_{kind}.parquet"),
                       compression="snappy")
        self.files += 1

    def write_silver(self, table: str, tick: int, arrays: dict, ops: np.ndarray | str = "c",
                     part: str | None = None):
        """The silver layer: full changed rows for this tick, one directory per tick.

        This is the blog's "deduplicated silver layer partitioned by updated_date" -- the
        feed an incremental job reads to answer "what changed since my checkpoint". It is
        shared upstream infrastructure (it is just landed CDC), so granting it costs the
        comparison nothing: the incremental engine is still billed for every byte it
        reads out of it, and the vanilla engine is free to ignore it because current-state
        tables are all a full refresh needs.

        Note what this does NOT give away. A changed row carries its own created_date, so
        the engine knows where the payment itself lives -- but nothing here says where its
        order or its card live, both of which sit on unrelated days by construction, and a
        changed offer arrives with no partition information at all. Those lookups are what
        the secondary indexes are for, and they stay expensive. Discounts are the
        deliberate exception: they co-partition with their payment, so their index is
        redundant, which the report states outright rather than quietly banking.
        """
        path = os.path.join(self.root, "silver", table, f"tick={tick:04d}")
        os.makedirs(path, exist_ok=True)
        n = len(next(iter(arrays.values())))
        cols = dict(arrays)
        # The CDC op code, exactly as Debezium would hand it over. It is what lets the
        # engine tell "this dimension row is brand new, its parent is in this batch too"
        # apart from "this dimension row changed, go find every fact row it touches".
        cols["op"] = np.full(n, ops) if isinstance(ops, str) else ops
        tbl = pa.table({k: pa.array(v) for k, v in cols.items()})
        # One file per call, not one per tick. A table can change more than once in a
        # cycle -- new cards AND re-issued cards -- and a fixed filename silently drops
        # whichever batch was written first, taking those rows out of the index and
        # leaving unresolvable joins downstream.
        name = f"part_{part}.parquet" if part else "part.parquet"
        pq.write_table(tbl, os.path.join(path, name), compression="snappy")
        self.files += 1


class Generator:
    """Holds the simulation's own state so updates can reference real history."""

    def __init__(self, scale: Scale, root: str, churn: str = "recent"):
        self.scale = scale
        self.churn = churn
        self.rng = np.random.default_rng(SEED)
        self.w = LakeWriter(root)
        self.root = root

        # Running entity state. These arrays are the generator's memory of what exists,
        # which is what lets it emit genuinely back-dated updates.
        self.pay_created: np.ndarray = np.zeros(0, dtype=np.int32)
        self.pay_order: np.ndarray = np.zeros(0, dtype=np.int64)
        self.pay_card: np.ndarray = np.zeros(0, dtype=np.int64)
        self.pay_merchant: np.ndarray = np.zeros(0, dtype=np.int64)
        self.pay_amount: np.ndarray = np.zeros(0, dtype=np.float64)

        self.card_created: np.ndarray = np.zeros(0, dtype=np.int32)
        self.n_payments = 0
        self.n_cards = 0
        self.n_discounts = 0
        self.stats: list[dict] = []

    # --- helpers ------------------------------------------------------------

    def _tick_days(self, tick: int) -> tuple[int, int]:
        """Which created_date values this tick's NEW payments occupy.

        Tick 0 lays down the whole backfill at once -- the Fact that already exists on
        day zero. Every tick after that is one day and one new partition.
        """
        if tick == 0:
            return 0, self.scale.backfill_days
        day = self.scale.backfill_days + tick - 1
        return day, day + 1

    def _n_new(self, tick: int) -> int:
        return self.scale.backfill_payments if tick == 0 else self.scale.new_per_tick

    def _sample_updates(self, tick: int) -> np.ndarray:
        """Payment ids to update this tick. See CHURN_MODELS in config.py.

        The "recent" model has two components, and the split is the whole ballgame:

          recent  payments from the last `recent_window` days, still moving through their
                  status transitions. Many rows, but concentrated in a few partitions.
          tail    a thin spray of genuinely back-dated corrections drawn uniformly from
                  ALL of history. Few rows, but each one dirties a whole partition.

        The tail is what stops this being a rigged demo -- it forces real index lookups
        and real scattered reads into year-old partitions. It is also, by count, what
        determines how much of the Fact has to be rewritten each cycle.
        """
        if self.n_payments == 0 or tick == 0:
            return np.zeros(0, dtype=np.int64)

        # replace=False throughout: one payment must appear at most once per tick. Two
        # rows for the same id in one tick would carry the SAME updated_at, and
        # latest_by_pk breaks ties arbitrarily -- the two engines would be free to pick
        # different winners and the correctness gate would fail on a data artefact.
        if self.churn == "uniform":
            n = min(int(self.n_payments * UNIFORM_UPDATE_FRAC), self.n_payments)
            if n == 0:
                return np.zeros(0, dtype=np.int64)
            return self.rng.choice(self.n_payments, size=n, replace=False).astype(np.int64)

        cur_day = self._tick_days(tick)[0]
        recent_mask = self.pay_created >= (cur_day - self.scale.recent_window)
        recent_pool = np.flatnonzero(recent_mask)

        n_recent = min(int(self.scale.recent_updates_per_new * self.scale.new_per_tick),
                       len(recent_pool))
        recent = (self.rng.choice(recent_pool, size=n_recent, replace=False)
                  if n_recent else np.zeros(0, dtype=np.int64))

        n_tail = min(int(self.scale.tail_update_rate * self.scale.new_per_tick),
                     self.n_payments)
        tail = self.rng.choice(self.n_payments, size=n_tail, replace=False)

        # np.unique because the two pools are drawn independently and DO overlap -- the
        # tail is uniform over all of history, which includes the recent window. Without
        # it a payment picked by both would be written twice in one tick with identical
        # updated_at, which is the tie latest_by_pk cannot resolve deterministically.
        return np.unique(np.concatenate([recent, tail]).astype(np.int64))

    # --- per-tick generation -------------------------------------------------

    def tick(self, tick: int) -> dict:
        day_lo, day_hi = self._tick_days(tick)
        n_new = self._n_new(tick)
        rng = self.rng

        # ---- inserts -------------------------------------------------------
        pay_ids = np.arange(self.n_payments, self.n_payments + n_new, dtype=np.int64)
        created = rng.integers(day_lo, day_hi, size=n_new).astype(np.int32)
        order_ids = pay_ids.copy()  # orders are 1:1 with payments
        merchants = rng.integers(0, N_MERCHANTS, size=n_new).astype(np.int64)
        amounts = np.round(rng.gamma(2.0, 900.0, size=n_new), 2)

        # Cards: some new, most reused. Reuse is what makes a card update fan out.
        n_new_cards = int(n_new * CARDS_PER_TICK_RATIO)
        new_card_ids = np.arange(self.n_cards, self.n_cards + n_new_cards, dtype=np.int64)
        # Drawn over the cards that exist AFTER this tick's insert, so a payment may point
        # at a card minted in the same batch. Drawing over self.n_cards instead would
        # leave every card unreferenced in the tick that created it, which no real ledger
        # does -- a card is used the moment it is issued.
        total_cards_after = self.n_cards + n_new_cards
        card_ids = rng.integers(0, max(1, total_cards_after), size=n_new).astype(np.int64)

        new_payments = {
            "id": pay_ids, "order_id": order_ids, "card_id": card_ids,
            "merchant_id": merchants, "amount": amounts,
            # Inserts take STATUSES[0:2]; updates below take STATUSES[2:]. The two sets are
            # kept disjoint on purpose, so an update ALWAYS changes the value that lands in
            # the fact. Drawn from one shared pool instead, a silently dropped update would
            # leave a still-correct-looking row roughly one time in five, and the gate
            # would catch the bug only sometimes.
            "status": STATUSES[rng.integers(0, 2, size=n_new)],
            "created_date": created, "updated_at": np.full(n_new, tick, dtype=np.int32),
        }
        self.w.write_partitioned("payments", tick, new_payments, created)

        # Orders are back-dated relative to their payment. The blog names this as the
        # thing that broke full refresh: "back-dated references (orders from years ago,
        # payments referencing old orders) forced full table scans on every secondary
        # table". Most payments settle against an order raised the same day, but a real
        # ledger has a long tail -- retries, subscriptions, saved orders, delayed capture.
        #
        # This decorrelation is what makes orders_index load-bearing. If an order always
        # shared its payment's partition, the payment's own created_date (which arrives
        # free in the silver row) would answer the lookup and the index would be
        # decoration -- a demo that proves nothing.
        # 85% settle same-day; the rest trail off over ~2 weeks, with a thin tail reaching
        # much further. Deliberately not extreme: with enough volume ANY diffuse tail makes
        # the union of referenced partitions cover the whole table, and then no index can
        # prune it. That outcome is real and the report calls it out per dimension -- but
        # it should come from a defensible distribution, not an inflated one.
        order_age = np.where(rng.random(n_new) < 0.85, 0,
                             rng.exponential(14.0, size=n_new)).astype(np.int32)
        # Clamped at 0: the exponential tail runs past the start of history. A negative
        # created_date would still be written and still be read back -- the glob is
        # recursive -- but it puts the partition key outside [0, total_days), which every
        # day-arithmetic and partition-accounting path in the demo assumes.
        order_created = np.clip(created - order_age, 0, None).astype(np.int32)
        new_orders = {
            "id": order_ids, "merchant_id": merchants,
            "receipt": np.char.add("rcpt_", order_ids.astype(str)),
            "created_date": order_created,
            "updated_at": np.full(n_new, tick, dtype=np.int32),
        }
        self.w.write_partitioned("orders", tick, new_orders, order_created)
        self.w.write_silver("orders", tick, new_orders)

        if n_new_cards:
            # Cards get their own created_date, deliberately decorrelated from the
            # payments that reference them. A payment's partition therefore tells you
            # nothing about where its card lives -- which is precisely why the forward
            # lookup needs cards_index rather than a lucky guess.
            card_created = rng.integers(max(0, day_lo - 30), day_hi,
                                        size=n_new_cards).astype(np.int32)
            new_cards = {
                "id": new_card_ids,
                "network": NETWORKS[rng.integers(0, len(NETWORKS), size=n_new_cards)],
                "last4": np.char.zfill(rng.integers(0, 10000, size=n_new_cards).astype(str), 4),
                "created_date": card_created,
                "updated_at": np.full(n_new_cards, tick, dtype=np.int32),
            }
            self.w.write_partitioned("cards", tick, new_cards, card_created)
            self.w.write_silver("cards", tick, new_cards, part="new")
            self.card_created = np.concatenate([self.card_created, card_created])

        # Card re-issues: a genuine dimension UPDATE, every cycle.
        #
        # Small in row count but expensive in reach -- cards are shared, so one corrected
        # card touches every payment that ever used it, scattered anywhere in history.
        # This is the routine expensive case in the blog's secondary flow, and without it
        # in the data the correctness gate would never test whether the engine handles a
        # changed dimension at all.
        n_card_upd = min(self.scale.card_updates, self.n_cards) if tick > 0 else 0
        if n_card_upd:
            upd_card_ids = rng.choice(self.n_cards, size=n_card_upd, replace=False)
            upd_card_created = self.card_created[upd_card_ids]
            upd_cards = {
                "id": upd_card_ids.astype(np.int64),
                "network": NETWORKS[rng.integers(0, len(NETWORKS), size=n_card_upd)],
                "last4": np.char.zfill(rng.integers(0, 10000, size=n_card_upd).astype(str), 4),
                "created_date": upd_card_created,
                "updated_at": np.full(n_card_upd, tick, dtype=np.int32),
            }
            self.w.write_partitioned("cards", tick, upd_cards, upd_card_created, kind="u")
            self.w.write_silver("cards", tick, upd_cards, ops="u", part="upd")

        # Discounts: attached to a subset of new payments, pointing at a shared offer.
        disc_mask = rng.random(n_new) < DISCOUNT_ATTACH_RATE
        n_disc = int(disc_mask.sum())
        if n_disc:
            # Discounts are raised with their payment, so they genuinely co-partition
            # with it. Left that way on purpose: the three dimensions now cover the three
            # regimes you actually meet, and the report names which one each fell into.
            #
            #   discounts  perfectly correlated  -> the index is redundant; the parent's
            #                                       own partition already answers it
            #   orders     decorrelated, long tail -> the index is load-bearing and earns
            #                                       its maintenance cost
            #   cards      referenced uniformly across all history -> the index cannot
            #                                       prune at all, and this column should
            #                                       be a runtime join, not denormalised
            disc_ids = np.arange(self.n_discounts, self.n_discounts + n_disc, dtype=np.int64)
            disc_created = created[disc_mask]
            new_discounts = {
                "id": disc_ids, "payment_id": pay_ids[disc_mask],
                "offer_id": rng.integers(0, N_OFFERS, size=n_disc).astype(np.int64),
                "amount": np.round(amounts[disc_mask] * 0.1, 2),
                "created_date": disc_created,
                "updated_at": np.full(n_disc, tick, dtype=np.int32),
            }
            self.w.write_partitioned("discounts", tick, new_discounts, disc_created)
            self.w.write_silver("discounts", tick, new_discounts)
            self.n_discounts += n_disc

        # ---- back-dated updates --------------------------------------------
        upd_ids = self._sample_updates(tick)
        n_upd = len(upd_ids)
        upd_payments = None
        if n_upd:
            upd_created = self.pay_created[upd_ids]
            upd_payments = {
                "id": upd_ids,
                "order_id": self.pay_order[upd_ids],
                "card_id": self.pay_card[upd_ids],
                "merchant_id": self.pay_merchant[upd_ids],
                "amount": self.pay_amount[upd_ids],
                # The mutation itself: a status transition, as a real payment would see.
                # Join keys are left alone -- a payment does not change which order it
                # belongs to -- which is what makes the indexes append-only. A dataset
                # with mutable foreign keys would add dedup cost to index maintenance.
                "status": STATUSES[rng.integers(2, len(STATUSES), size=n_upd)],
                "created_date": upd_created,
                "updated_at": np.full(n_upd, tick, dtype=np.int32),
            }
            self.w.write_partitioned("payments", tick, upd_payments, upd_created, kind="u")

        # ---- offers: created once, mutated on scripted fan-out ticks --------
        offers_changed = np.zeros(0, dtype=np.int64)
        if tick == 0:
            offer_ids = np.arange(N_OFFERS, dtype=np.int64)
            all_offers = {
                "id": offer_ids,
                "name": np.char.add("offer_", offer_ids.astype(str)),
                "percent": np.round(rng.uniform(1, 30, size=N_OFFERS), 2),
                "updated_at": np.zeros(N_OFFERS, dtype=np.int32),
            }
            self.w.write_unpartitioned("offers", tick, all_offers)
            self.w.write_silver("offers", tick, all_offers)
            offers_changed = offer_ids
        elif tick in self.scale.fanout_ticks:
            # One hot offer. ~1/N_OFFERS of every discount ever written points at it, so
            # this single row touches fact rows scattered across the entire history --
            # the write-amplification case that makes incremental briefly worse.
            hot = np.array([0], dtype=np.int64)
            hot_offer = {
                "id": hot, "name": np.array([f"offer_0_revised_t{tick}"]),
                "percent": np.round(rng.uniform(1, 30, size=1), 2),
                "updated_at": np.array([tick], dtype=np.int32),
            }
            self.w.write_unpartitioned("offers", tick, hot_offer, kind="u")
            self.w.write_silver("offers", tick, hot_offer, ops="u")
            offers_changed = hot

        # ---- silver: payments (inserts + back-dated updates together) --------
        if upd_payments:
            silver_payments = {k: np.concatenate([new_payments[k], upd_payments[k]])
                               for k in new_payments}
            silver_ops = np.concatenate([np.full(n_new, "c"),
                                         np.full(len(upd_ids), "u")])
        else:
            silver_payments, silver_ops = new_payments, "c"
        self.w.write_silver("payments", tick, silver_payments, ops=silver_ops)

        # ---- advance state ---------------------------------------------------
        self.pay_created = np.concatenate([self.pay_created, created])
        self.pay_order = np.concatenate([self.pay_order, order_ids])
        self.pay_card = np.concatenate([self.pay_card, card_ids])
        self.pay_merchant = np.concatenate([self.pay_merchant, merchants])
        self.pay_amount = np.concatenate([self.pay_amount, amounts])
        self.n_payments += n_new
        self.n_cards = total_cards_after

        return {
            "tick": tick, "new_payments": n_new, "updated_payments": n_upd,
            "total_payments": self.n_payments,
            "distinct_update_partitions": int(len(np.unique(self.pay_created[upd_ids]))) if n_upd else 0,
            "total_partitions": int(len(np.unique(self.pay_created))),
            "offers_changed": int(len(offers_changed)),
            "day_range": [day_lo, day_hi],
        }

    def run(self) -> dict:
        for t in range(self.scale.ticks):
            self.stats.append(self.tick(t))
        manifest = {
            "scale": self.scale.name,
            "churn": self.churn,
            "backfill_payments": self.scale.backfill_payments,
            "backfill_days": self.scale.backfill_days,
            "tail_update_rate": self.scale.tail_update_rate,
            "tail_updates_per_tick": int(self.scale.tail_update_rate * self.scale.new_per_tick),
            "recent_window": self.scale.recent_window,
            "card_updates_per_tick": self.scale.card_updates,
            "seed": SEED,
            "ticks": self.scale.ticks,
            "total_days": self.scale.total_days,
            "fanout_ticks": list(self.scale.fanout_ticks),
            "total_payments": self.n_payments,
            "files_written": self.w.files,
            "per_tick": self.stats,
        }
        with open(os.path.join(self.root, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2)
        return manifest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", default="demo", choices=sorted(SCALES))
    ap.add_argument("--churn", default="recent", choices=CHURN_MODELS,
                    help="recent = realistic constant-volume churn; "
                         "uniform = adversarial, a fixed share of the whole table each tick")
    ap.add_argument("--lake", default="/lake")
    args = ap.parse_args()

    scale = SCALES[args.scale]
    # Clear the CONTENTS, not the directory: in the container /lake is a bind mount and
    # removing it raises EBUSY.
    os.makedirs(args.lake, exist_ok=True)
    for name in os.listdir(args.lake):
        path = os.path.join(args.lake, name)
        shutil.rmtree(path) if os.path.isdir(path) else os.remove(path)

    m = Generator(scale, args.lake, churn=args.churn).run()
    print(f"scale={m['scale']} churn={m['churn']} ticks={m['ticks']} "
          f"payments={m['total_payments']:,} "
          f"files={m['files_written']:,}")
    last = m["per_tick"][-1]
    print(f"final tick: {last['updated_payments']:,} updates dirtying "
          f"{last['distinct_update_partitions']} of {last['total_partitions']} partitions "
          f"({last['distinct_update_partitions'] / max(last['total_partitions'], 1):.0%})")


if __name__ == "__main__":
    main()
