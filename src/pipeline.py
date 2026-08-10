"""Shared machinery. Both engines are constructed through here so neither can end up
with a Spark setting, a parquet codec, or a dedup implementation the other doesn't have.

The one thing an engine controls is *which files it asks for*. That is the entire
experiment, so everything else is pinned.
"""

from __future__ import annotations

import glob
import hashlib
import json
import os
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from .metrics import TickMetrics

# --- schemas ----------------------------------------------------------------
# Declared rather than inferred: inference cost would otherwise vary with how many
# files each engine touches, which would quietly bias the comparison.


def _f(name, t, nullable=True):
    return StructField(name, t, nullable)


SCHEMAS = {
    "payments": StructType([
        _f("id", LongType()), _f("order_id", LongType()), _f("card_id", LongType()),
        _f("merchant_id", LongType()), _f("amount", DoubleType()), _f("status", StringType()),
        _f("created_date", IntegerType()), _f("updated_at", IntegerType()),
    ]),
    "orders": StructType([
        _f("id", LongType()), _f("merchant_id", LongType()), _f("receipt", StringType()),
        _f("created_date", IntegerType()), _f("updated_at", IntegerType()),
    ]),
    "cards": StructType([
        _f("id", LongType()), _f("network", StringType()), _f("last4", StringType()),
        _f("created_date", IntegerType()), _f("updated_at", IntegerType()),
    ]),
    "discounts": StructType([
        _f("id", LongType()), _f("payment_id", LongType()), _f("offer_id", LongType()),
        _f("amount", DoubleType()), _f("created_date", IntegerType()),
        _f("updated_at", IntegerType()),
    ]),
    "offers": StructType([
        _f("id", LongType()), _f("name", StringType()), _f("percent", DoubleType()),
        _f("updated_at", IntegerType()),
    ]),
}

# The silver layer is the source row plus the CDC op code ("c" = created, "u" = updated).
# The op is what tells a dimension change apart from a dimension insert: an insert's
# parent is in the same batch and needs no back-traversal, while an update can touch fact
# rows scattered across the whole history.
SILVER_SCHEMAS = {
    name: StructType(list(schema.fields) + [_f("op", StringType())])
    for name, schema in SCHEMAS.items()
}


# --- spark session ----------------------------------------------------------

def spark_conf(threads: int, driver_mem: str, shuffle_partitions: int,
               local_dir: str) -> dict[str, str]:
    """The settings both engines run under. Changing anything here changes it for both.

    Nothing is disabled to make the baseline look bad: adaptive execution, broadcast
    joins and vectorised parquet reads are all left on, exactly as a competent engineer
    would leave them.
    """
    return {
        "spark.master": f"local[{threads}]",
        "spark.driver.memory": driver_mem,
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.parquet.compression.codec": "snappy",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.parquet.vectorizedReader.enabled": "true",
        "spark.local.dir": local_dir,
        "spark.ui.enabled": "false",
        "spark.driver.bindAddress": "127.0.0.1",
        "spark.driver.host": "127.0.0.1",
        # Keep the driver from being killed by a runaway broadcast rather than by the
        # thing we want to observe. This is the Spark default; stated for the record.
        "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024),
    }


def build_spark(app_name: str, conf: dict[str, str]) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def conf_fingerprint(conf: dict[str, str]) -> str:
    """Both engines record this. If the two runs disagree, the comparison is void."""
    payload = json.dumps({k: v for k, v in sorted(conf.items())
                          if k not in ("spark.local.dir",)}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


# --- lake reader ------------------------------------------------------------

class LakeReader:
    """Reads parquet by explicit file path and bills every byte to the caller.

    Passing explicit paths is what makes partition pruning honest here. An engine that
    wants one partition lists one directory and pays for those files; an engine that
    wants everything lists everything and pays for everything. There is no way to read a
    file without it showing up in the bill.
    """

    def __init__(self, spark: SparkSession, lake_dir: str):
        self.spark = spark
        self.lake_dir = lake_dir
        self.files_read = 0
        self.bytes_read = 0
        self.partitions_read = 0
        # The whole run is generated up front so both engines replay byte-identical
        # input, but an engine at tick N must only see files from ticks <= N. Filenames
        # carry their producing tick (t0007_i.parquet), which is how the lake is made to
        # grow over time without regenerating anything.
        self.as_of_tick = 10**9

    def reset(self) -> None:
        self.files_read = 0
        self.bytes_read = 0
        self.partitions_read = 0

    @staticmethod
    def _tick_of(path: str) -> int:
        """t0007_u.parquet -> 7"""
        name = os.path.basename(path)
        try:
            return int(name.split("_", 1)[0][1:])
        except ValueError:
            return -1

    def _visible(self, paths: list[str]) -> list[str]:
        return [p for p in paths if self._tick_of(p) <= self.as_of_tick]

    def bill(self, paths: list[str], partitions: int) -> None:
        """Charge these files to the current cycle.

        Public because the engines read two things this class does not open for them --
        the incremental engine's own secondary indexes, and the fact partitions it is
        about to rewrite -- and both must land on the same bill as everything else. An
        engine that could read a file off the books would make the whole comparison
        meaningless, so there is exactly one place bytes are counted and it is this one.
        """
        self.files_read += len(paths)
        self.partitions_read += partitions
        for p in paths:
            try:
                self.bytes_read += os.path.getsize(p)
            except OSError:
                pass

    def _empty(self, schema: StructType) -> DataFrame:
        """An empty frame carrying exactly `schema`, built entirely on the JVM.

        spark.createDataFrame([], schema) is the obvious call and it is a trap: it routes
        through a Python RDD, which starts a Python worker, which hard-crashes the job
        when PYSPARK_PYTHON resolves to a different interpreter than the driver (easy to
        hit outside the container, where `python3` on PATH need not be the venv's).
        Nothing here needs Python execution at all -- range(0) plus one typed null literal
        per field yields the identical schema without ever leaving the JVM.
        """
        return self.spark.range(0).select(
            *[F.lit(None).cast(f.dataType).alias(f.name) for f in schema.fields])

    def partition_dirs(self, table: str) -> list[int]:
        base = os.path.join(self.lake_dir, "source", table)
        if not os.path.isdir(base):
            return []
        out = []
        for name in os.listdir(base):
            if name.startswith("created_date="):
                out.append(int(name.split("=", 1)[1]))
        return sorted(out)

    def read_source(self, table: str, days: list[int] | None = None) -> DataFrame:
        """days=None means the whole table. That is the full-refresh access pattern and
        it costs what it costs."""
        base = os.path.join(self.lake_dir, "source", table)
        schema = SCHEMAS[table]
        if not os.path.isdir(base):
            return self._empty(schema)

        if days is None:
            paths = self._visible(
                sorted(glob.glob(os.path.join(base, "**", "*.parquet"), recursive=True)))
            n_parts = len(self.partition_dirs(table)) or 1
        else:
            paths, n_parts = [], 0
            for d in sorted(set(days)):
                part = os.path.join(base, f"created_date={d}")
                found = self._visible(sorted(glob.glob(os.path.join(part, "*.parquet"))))
                if found:
                    paths.extend(found)
                    n_parts += 1

        if not paths:
            return self._empty(schema)
        self.bill(paths, n_parts)
        return self.spark.read.schema(schema).parquet(*paths)

    def read_silver(self, table: str, ticks: list[int]) -> DataFrame:
        """Rows that changed in the given ticks -- the "what changed since my checkpoint"
        feed. Cheap by construction: one small directory per tick, no scanning."""
        base = os.path.join(self.lake_dir, "silver", table)
        schema = SILVER_SCHEMAS[table]
        paths = []
        for t in ticks:
            paths.extend(sorted(glob.glob(os.path.join(base, f"tick={t:04d}", "*.parquet"))))
        if not paths:
            return self._empty(schema)
        self.bill(paths, len(paths))
        return self.spark.read.schema(schema).parquet(*paths)


# --- fact table I/O ---------------------------------------------------------
# Both engines write the fact through these, so the two outputs are directly comparable
# and neither gets a friendlier layout or codec than the other.

FACT_PARTITION_COL = "created_date"


def write_fact(df: DataFrame, fact_dir: str) -> None:
    """Write partitioned by created_date.

    With spark.sql.sources.partitionOverwriteMode=dynamic this replaces exactly the
    partitions present in `df` and leaves the rest untouched. The full refresh writes
    every partition, so it replaces everything; the incremental engine writes only the
    partitions it touched. Same call, and the difference in what it costs is the point.

    repartition(created_date) first so each partition is written by one task and lands as
    one file. Without it every shuffle partition holding rows for a date emits its own
    fragment into that directory, multiplying the file count by up to the shuffle width --
    and the incremental engine re-reads those same partitions to merge into them next
    cycle, so every extra fragment shows up again as files_read on its own bill.
    """
    (df.repartition(FACT_PARTITION_COL)
       .write.mode("overwrite")
       .partitionBy(FACT_PARTITION_COL)
       .parquet(fact_dir))


def fact_partition_paths(fact_dir: str, days: list[int]) -> list[str]:
    paths = []
    for d in sorted(set(days)):
        paths.extend(sorted(glob.glob(os.path.join(fact_dir, f"{FACT_PARTITION_COL}={d}",
                                                   "*.parquet"))))
    return paths


def read_fact_partitions(spark: SparkSession, fact_dir: str, paths: list[str],
                         columns: list[str]) -> DataFrame | None:
    """Read specific fact partitions by path. basePath lets Spark recover created_date
    from the directory names, since a partition column is not stored inside the files."""
    if not paths:
        return None
    return (spark.read.option("basePath", fact_dir).parquet(*paths)).select(*columns)


# --- shared transforms ------------------------------------------------------

def latest_by_pk(df: DataFrame, pk: str) -> DataFrame:
    """Collapse an append-only table to current state: newest row per key.

    Both engines call this. Vanilla applies it to the entire lake every cycle;
    incremental applies it to the handful of partitions it touched. Same code, and the
    difference in cost is exactly the point being measured.
    """
    # Window + row_number rather than groupBy(pk).max("updated_at") joined back: the
    # groupBy form needs a second pass over the same rows to recover the payload columns,
    # and it emits BOTH versions when two share an updated_at. row_number keeps exactly
    # one row per key regardless, so the row COUNT never depends on the input being free
    # of ties. Which of a tied pair survives is still arbitrary, and the two engines
    # evaluate this over different sets of partitions -- so they could disagree. That is
    # why the generator guarantees at most one row per key per tick (see _sample_updates).
    w = Window.partitionBy(pk).orderBy(F.col("updated_at").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn"))


# --- engine base ------------------------------------------------------------

class FactPipeline(ABC):
    name: str = "base"

    def __init__(self, spark: SparkSession, lake_dir: str, out_dir: str):
        self.spark = spark
        self.lake_dir = lake_dir
        self.out_dir = out_dir
        self.reader = LakeReader(spark, lake_dir)
        self.fact_dir = os.path.join(out_dir, "payments_fact")
        os.makedirs(out_dir, exist_ok=True)
        # Needed by MemorySampler on a dev laptop, where there is no cgroup and the JVM
        # is a child process the Python-side rusage counters never see.
        try:
            self.jvm_pid = spark.sparkContext._gateway.proc.pid
        except Exception:
            self.jvm_pid = None

    @abstractmethod
    def run_tick(self, tick: int) -> TickMetrics:
        """Do one refresh cycle and report what it cost."""

    def _begin_tick(self, tick: int) -> None:
        """Advance the visible lake to this tick and clear the I/O bill."""
        self.reader.reset()
        self.reader.as_of_tick = tick
