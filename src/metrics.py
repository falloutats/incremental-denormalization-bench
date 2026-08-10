"""Measurement. Everything here is deliberately literal -- no estimates, no models.

Two numbers do the arguing in this demo and both are measured rather than inferred:

  bytes_read       summed sizes of the parquet files an engine actually opened. Not a row
                   count, not a guess. This is what makes "vanilla re-reads the whole lake
                   every cycle" a fact rather than a claim.
  peak_rss_bytes   the highest unreclaimable memory OCCUPANCY reached during the cycle,
                   in the same units the docker ceiling is enforced in. See below.

The memory family of fields, precisely
--------------------------------------
All of them are bytes, all of them are about the container's cgroup (or, on a laptop with
no cgroup, the RSS of this process plus the Spark JVM).

  peak_rss_bytes         Peak OCCUPANCY during the cycle: the highest unreclaimable
                         footprint the cgroup held at any instant between the start and
                         the end of the tick. It INCLUDES memory that was already held
                         when the tick opened, because that memory is genuinely occupied
                         and genuinely counts against the same ceiling -- the JVM is
                         long-lived and never hands heap back to the OS. That also means
                         this curve is a ratchet: it is non-decreasing largely because
                         the JVM's floor is non-decreasing, NOT because each cycle demands
                         more. Read it as "how close is the container to the wall", never
                         as "how much did this cycle cost".
                         Accuracy: when it comes from the poller it is a strict lower
                         bound -- a shorter spike can only have been higher. When it comes
                         from the kernel mark it is the kernel's exact high-water of
                         memory.current with the largest page cache we OBSERVED subtracted
                         off; since that cache figure is itself sampled, a cache spike
                         between two polls could leave a little cache attributed as
                         footprint. mem_kernel_peak_bytes and mem_sampled_peak_bytes are
                         both emitted raw so the size of any such gap is visible.
  mem_start_bytes        Occupancy at the instant the tick opened -- the JVM's floor going
                         in. peak_rss_bytes - mem_start_bytes is what this cycle added.
  mem_peak_growth_bytes  Exactly that difference, precomputed. THIS is the per-cycle cost
                         number: it is not a ratchet and it is what to use when comparing
                         what two engines demand of a cycle.
  mem_peak_source        Where peak_rss_bytes came from, so a reader can judge it:
                           cgroup.memory.peak/reset  kernel high-water, our own fd's
                                                     watermark, reset at tick start
                           cgroup.memory.peak/delta  kernel high-water, monotonic since
                                                     cgroup creation, and it advanced
                                                     during this tick so the new value is
                                                     this tick's peak
                           cgroup.sampled            100ms poll of the cgroup workingset
                                                     only; the kernel mark was unavailable
                                                     or did not advance, so a spike shorter
                                                     than the poll interval could have been
                                                     missed
                           ps.sampled                no cgroup at all (dev laptop), RSS of
                                                     this process + the Spark JVM
  mem_sampled_peak_bytes The polled maximum on its own, always populated. Comparing it
                         with peak_rss_bytes shows how much the poller missed.
  mem_kernel_peak_bytes  The kernel's high-water for this tick as the kernel reports it,
                         i.e. of memory.current, page cache INCLUDED. 0 when unusable.
  mem_anon_peak_bytes    Highest memory.stat `anon` seen: the most direct measure of the
                         JVM heap + off-heap. Nothing reclaimable is in it.
  mem_run_peak_bytes     memory.peak at end of tick: high-water of memory.current since
                         the container started, cache included. Run-level context only.

Why the kernel's mark matters: a 100ms python poll can miss a spike entirely, and short
spikes are exactly what kill a JVM. memory.peak is maintained by the kernel on every
charge, so it cannot miss one. It is monotonic since cgroup creation, so it is never read
naively per-tick -- it is either reset per-tick (Linux 6.12+, and only when /sys/fs/cgroup
is writable, which under docker it usually is not) or differenced across the tick.

The kernel's own verdict on whether any of this was fatal is recorded separately, by
oom_kill_count().
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field

CGROUP_CURRENT = "/sys/fs/cgroup/memory.current"
CGROUP_STAT = "/sys/fs/cgroup/memory.stat"
CGROUP_MAX = "/sys/fs/cgroup/memory.max"
CGROUP_EVENTS = "/sys/fs/cgroup/memory.events"
CGROUP_PEAK = "/sys/fs/cgroup/memory.peak"


def _read_int(path: str) -> int | None:
    try:
        with open(path) as f:
            v = f.read().strip()
        return None if v == "max" else int(v)
    except (OSError, ValueError):
        return None


def memory_limit_bytes() -> int | None:
    """The hard ceiling docker is enforcing, straight from the cgroup."""
    return _read_int(CGROUP_MAX)


def cgroup_memory() -> dict | None:
    """One reading of this cgroup's memory accounting. None when there is no cgroup.

    Keys, all bytes:
      current     memory.current -- everything charged to the cgroup, page cache included.
      cache       the part of that which the kernel can reclaim instead of OOM-killing.
      anon        anonymous memory (JVM heap and off-heap, python objects). Unreclaimable
                  here: both containers run memswap_limit == mem_limit, i.e. no swap, so
                  nothing can page anon out.
      workingset  what to compare against memory.max for OOM purposes: current - cache.

    Two details that decide whether this number means anything:

    Why subtract cache at all. `memory.current` includes the page cache of every file the
    container has touched, and the full refresh streams the entire lake every cycle while
    the incremental engine reads a fraction -- so charting memory.current would show the
    full refresh "growing into the ceiling" purely because it read more files, which is
    not memory pressure at all. The kernel drops that cache rather than OOM-killing.

    Why cache is NOT simply active_file + inactive_file. tmpfs/shmem pages are charged to
    the file LRUs and counted in those two fields, but with swap off they cannot be
    reclaimed -- they are as fatal as anon. Subtracting them would hide real pressure, and
    it is not hypothetical: point spark.local.dir at a tmpfs and every shuffle file lands
    here. So shmem is added back.

    Note the ceiling itself is enforced against memory.current, cache included. Exceeding
    memory.max with reclaimable cache costs *time* (the kernel reclaims in the allocating
    task's context) but does not kill; only unreclaimable memory does. That is why the
    chart is drawn in workingset and the kill is reported separately by oom_kill_count().
    """
    stat: dict[str, int] = {}
    try:
        with open(CGROUP_STAT) as f:
            for line in f:
                key, _, value = line.partition(" ")
                try:
                    stat[key] = int(value)
                except ValueError:
                    continue
    except OSError:
        stat = {}
    # Read current AFTER memory.stat so that, while memory is growing, `current` is the
    # fresher of the two and the subtraction below cannot be inflated by a stale total.
    current = _read_int(CGROUP_CURRENT)
    if current is None:
        return None

    anon = stat.get("anon", 0)
    cache = max(0, stat.get("active_file", 0) + stat.get("inactive_file", 0)
                - stat.get("shmem", 0))
    # Both terms are lower bounds on the unreclaimable footprint -- `current - cache` can
    # be dragged low by a torn read across the two files, and `anon` omits kernel
    # structures -- so take the better of them. This is also what stops the old
    # max(0, ...) clamp from ever reporting a nonsensical 0 while a 1.5GB heap is resident.
    workingset = max(anon, current - cache)
    return {"current": current, "cache": cache, "anon": anon,
            "workingset": min(current, max(0, workingset))}


def cgroup_workingset() -> int | None:
    """Unreclaimable memory in this cgroup right now. None off-cgroup. See cgroup_memory."""
    mem = cgroup_memory()
    return None if mem is None else mem["workingset"]


def oom_kill_count() -> int:
    """How many times the kernel has OOM-killed something in this cgroup."""
    try:
        with open(CGROUP_EVENTS) as f:
            for line in f:
                if line.startswith("oom_kill "):
                    return int(line.split()[1])
    except OSError:
        pass
    return 0


class MemorySampler:
    """Peak memory occupancy across the `with` block. See this module's docstring.

    Two sources, and it uses whichever can prove the larger number:

      the kernel's high-water mark (memory.peak), which cannot miss a spike because the
      kernel updates it on every charge -- but which counts page cache, so converting it
      to the workingset scale means subtracting the largest cache seen during the tick.
      Usually pessimistic: cache at the peak instant is unknown and is typically smaller
      than the tick's maximum, especially near the ceiling where the kernel has just
      reclaimed. The one way it can flatter the number is a cache spike that lands between
      two polls, so both raw inputs are reported and never silently merged.

      a 100ms poll of the cgroup's workingset, which is on the right scale already but can
      miss a spike shorter than the interval -- a strict lower bound.

    Both under-report far more often than they over-report, which is what makes taking the
    max of them the right rule. mem_peak_source records which one won.

    `peak` is seeded with the occupancy at the start of the block on purpose: the JVM is
    long-lived, memory it is already holding is really there, and the ceiling does not care
    when it was allocated. What that seeding must NOT be allowed to do is masquerade as
    this cycle's cost -- hence `start` and `growth` alongside it.

    On a dev laptop there is no cgroup, so it falls back to summing the RSS of this process
    and the Spark JVM it spawned -- approximate, and only ever used outside the measured
    container runs.
    """

    def __init__(self, jvm_pid: int | None = None, interval: float = 0.1):
        self.jvm_pid = jvm_pid
        self.interval = interval
        self.peak = 0            # best lower bound on peak occupancy during the block
        self.start = 0           # occupancy when the block opened
        self.growth = 0          # peak - start: what this block actually added
        self.sampled_peak = 0    # what the poller alone saw
        self.kernel_peak = 0     # kernel high-water for this block, cache INCLUDED
        self.run_peak = 0        # kernel high-water since cgroup creation, cache INCLUDED
        self.anon_peak = 0       # highest memory.stat anon seen
        self.source = ""
        self._max_cache = 0      # largest reclaimable cache seen, for the conversion above
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._cgroup = os.path.exists(CGROUP_CURRENT)
        self._peak_fd = None
        self._kernel_start: int | None = None

    def _sample(self) -> int:
        """One observation of the workingset. Updates the cache/anon marks as a side
        effect, since they come from the same read and both are needed at exit."""
        if self._cgroup:
            mem = cgroup_memory()
            if mem is None:
                return 0
            self._max_cache = max(self._max_cache, mem["cache"])
            self.anon_peak = max(self.anon_peak, mem["anon"])
            return mem["workingset"]
        # Host fallback: RSS of this process plus the Spark JVM. Also excludes page
        # cache, so the two run modes measure comparable quantities.
        pids = [str(os.getpid())] + ([str(self.jvm_pid)] if self.jvm_pid else [])
        try:
            out = subprocess.run(["ps", "-o", "rss=", "-p", ",".join(pids)],
                                 capture_output=True, text=True, timeout=2).stdout
            rss = sum(int(x) for x in out.split()) * 1024
        except (subprocess.SubprocessError, ValueError):
            return 0
        self.anon_peak = max(self.anon_peak, rss)
        return rss

    def _open_resettable_peak(self):
        """A private, resettable view of memory.peak, or None if the kernel won't give one.

        Linux 6.12+ gives every open fd on memory.peak its own watermark and resets that
        fd's watermark when you write to it, which yields a per-tick kernel peak directly
        -- provided the fd stays open for the whole tick, since the watermark belongs to
        the fd and not to the path. Older kernels reject the write, and so does docker's
        read-only /sys/fs/cgroup mount, which is the common case here. Callers fall back
        to differencing the monotonic value.
        """
        try:
            f = open(CGROUP_PEAK, "r+")
        except OSError:
            return None
        try:
            f.write("0")
            f.flush()
            return f
        except OSError:
            f.close()
            return None

    def _read_fd(self, f) -> int | None:
        try:
            f.seek(0)
            return int(f.read().strip())
        except (OSError, ValueError):
            return None

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            self.sampled_peak = max(self.sampled_peak, self._sample())

    def __enter__(self) -> MemorySampler:
        # Every accumulator is per-block, so reset them here rather than in __init__:
        # one sampler reused for two ticks must not carry the first tick's marks.
        self._max_cache = self.anon_peak = self.kernel_peak = self.run_peak = 0
        self.source = ""
        self._kernel_start = None
        self.start = self._sample()
        self.sampled_peak = self.start
        self.peak = self.start
        if self._cgroup:
            self._peak_fd = self._open_resettable_peak()
            if self._peak_fd is None:
                # Fall back to differencing: remember where the monotonic mark stood.
                self._kernel_start = _read_int(CGROUP_PEAK)
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1)
        self.sampled_peak = max(self.sampled_peak, self._sample())
        self.peak = self.sampled_peak
        self.source = "cgroup.sampled" if self._cgroup else "ps.sampled"

        self.run_peak = (_read_int(CGROUP_PEAK) or 0) if self._cgroup else 0
        kernel_tick_peak, mode = None, ""
        if self._peak_fd is not None:
            kernel_tick_peak, mode = self._read_fd(self._peak_fd), "reset"
            self._peak_fd.close()
            self._peak_fd = None
        elif self._kernel_start is not None and self.run_peak > self._kernel_start:
            # The monotonic mark advanced during this tick, so its new value IS this
            # tick's peak. If it did not advance, all the kernel tells us is "<= the
            # run's high-water", which is not a per-tick measurement -- we keep the
            # sampled number and say so in the source.
            kernel_tick_peak, mode = self.run_peak, "delta"

        if kernel_tick_peak is not None:
            self.kernel_peak = kernel_tick_peak
            # memory.peak is a high-water of memory.current, page cache included. The
            # cache at that instant is unknown; it was at most the largest cache we saw,
            # so this is the workingset floor implied by the kernel's number.
            implied = max(0, kernel_tick_peak - self._max_cache)
            if implied > self.peak:
                self.peak = implied
                self.source = f"cgroup.memory.peak/{mode}"

        # peak is seeded with start and only ever raised, so this cannot go negative today;
        # the clamp is here because growth is published as a cost and a negative one would
        # be read as this cycle having freed memory, which no source above can prove.
        self.growth = max(0, self.peak - self.start)

    def detail(self) -> dict:
        """The TickMetrics memory fields this measurement supports."""
        return {
            "peak_rss_bytes": self.peak,
            "mem_peak_source": self.source,
            "mem_start_bytes": self.start,
            "mem_peak_growth_bytes": self.growth,
            "mem_sampled_peak_bytes": self.sampled_peak,
            "mem_kernel_peak_bytes": self.kernel_peak,
            "mem_anon_peak_bytes": self.anon_peak,
            "mem_run_peak_bytes": self.run_peak,
        }


def parquet_row_count(path: str) -> int:
    """Row count from parquet footers -- metadata only, no data pages touched.

    Counting with Spark instead would add a full scan of the fact to every cycle, which
    would land hardest on whichever engine has the biggest fact and quietly distort the
    thing being measured.
    """
    import pyarrow.parquet as pq

    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            if name.endswith(".parquet"):
                try:
                    total += pq.ParquetFile(os.path.join(root, name)).metadata.num_rows
                except Exception:
                    pass
    return total


def dir_size(path: str) -> int:
    """Total bytes under a directory: the fact's size, and the scratch delta.

    Not a spill measurement -- see TickMetrics.scratch_bytes for why the difference
    between two of these readings cannot be attributed to spill alone.
    """
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


@dataclass
class TickMetrics:
    """One row per engine per cycle. The dashboard renders nothing but these."""

    tick: int
    engine: str
    wall_s: float = 0.0
    # I/O -- the core of the argument
    files_read: int = 0
    bytes_read: int = 0
    partitions_read: int = 0
    # work done
    rows_written: int = 0
    # Rows physically rewritten, including untouched rows carried along in a replaced
    # partition. rows_rewritten / rows_written is the partition-rewrite amplification.
    rows_rewritten: int = 0
    fact_rows_total: int = 0
    fact_bytes_total: int = 0
    # resources. peak_rss_bytes is peak OCCUPANCY, floor included -- the module docstring
    # defines every one of these exactly. mem_peak_growth_bytes, not peak_rss_bytes, is
    # the per-cycle cost. The mem_* fields are filled in by MetricsWriter from the
    # MemorySampler that produced peak_rss_bytes, because the engines set only that one.
    peak_rss_bytes: int = 0
    mem_limit_bytes: int | None = None
    mem_peak_source: str = ""
    mem_start_bytes: int = 0
    mem_peak_growth_bytes: int = 0
    mem_sampled_peak_bytes: int = 0
    mem_kernel_peak_bytes: int = 0
    mem_anon_peak_bytes: int = 0
    mem_run_peak_bytes: int = 0
    # Bytes left behind in spark.local.dir across the cycle. This is shuffle output AND
    # spill, and Spark's ContextCleaner removes both on its own schedule -- so it is a
    # rough indicator of local-disk pressure, NOT a clean measurement of spill. Named for
    # what it is so nothing downstream claims more than it can support.
    scratch_bytes: int = 0
    # honest-costs breakdown for the incremental engine (0 for vanilla)
    index_maint_s: float = 0.0
    changed_rows: int = 0
    fanout_rows: int = 0
    # bookkeeping
    status: str = "ok"  # ok | meta | oom | error
    error: str = ""
    lag_s: float = 0.0
    notes: dict = field(default_factory=dict)


class MetricsWriter:
    """Appends one JSON object per line, flushed immediately so the live dashboard
    on the host sees each tick the moment it lands."""

    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self._f = open(path, "w", buffering=1)

    def write(self, m: TickMetrics) -> None:
        self._f.write(json.dumps(asdict(m)) + "\n")
        self._f.flush()
        # The flush above is what the tailing dashboard sees: once the bytes reach the
        # kernel they are visible to every reader of that inode, bind mount or not, and
        # they survive this process being SIGKILLed by the OOM killer -- the kernel owns
        # the writeback. fsync buys only durability against the HOST dying, which is cheap
        # insurance for one line per cycle and the reason the last tick before a hard
        # failure is still on disk.
        os.fsync(self._f.fileno())

    def close(self) -> None:
        self._f.close()


class Timer:
    def __enter__(self) -> Timer:
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *exc) -> None:
        self.elapsed = time.perf_counter() - self.t0
