#!/usr/bin/env bash
# Build, generate, race both engines under identical caps, then verify and report.
#
#   ./run.sh                       demo scale, one engine at a time, live dashboard
#   ./run.sh --scale stress        the largest preset: 6M-row backfill under the same cap
#   ./run.sh --churn uniform       the adversarial regime where incremental stops winning
#   ./run.sh --parallel            both engines at once, for the side-by-side race
#   ./run.sh --mem 2g --cpus 2     change the box both engines run in
#
# Sequential is the default because it is the mode whose numbers can be trusted: the two
# containers are separately capped but still share one disk, and nothing in either cap
# accounts for that contention. --parallel is the demo mode -- watching the two curves
# diverge live is more legible than reading them one after the other -- and its timings
# are only as good as the contention allows.

set -euo pipefail
cd "$(dirname "$0")"
source ./env.sh

SCALE=demo
CHURN=recent
MODE=sequential
SKIP_GEN=0
export ENGINE_MEM=${ENGINE_MEM:-3g}
export ENGINE_CPUS=${ENGINE_CPUS:-2.0}
export DRIVER_MEM=${DRIVER_MEM:-1400m}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scale) SCALE="$2"; shift 2;;
    --churn) CHURN="$2"; shift 2;;
    --sequential) MODE=sequential; shift;;
    --parallel) MODE=parallel; shift;;
    --mem) export ENGINE_MEM="$2"; shift 2;;
    --cpus) export ENGINE_CPUS="$2"; shift 2;;
    --driver-mem) export DRIVER_MEM="$2"; shift 2;;
    --skip-gen) SKIP_GEN=1; shift;;
    # --help reprints the header block verbatim. The line range is hardcoded, so adding or
    # removing a line up there silently truncates the help text -- keep 2,14 in sync.
    -h|--help) sed -n '2,14p' "$0"; exit 0;;
    *) echo "unknown flag: $1" >&2; exit 2;;
  esac
done
export SCALE CHURN

green() { printf '\033[32m%s\033[0m\n' "$1"; }
red() { printf '\033[31m%s\033[0m\n' "$1"; }

if ! docker info >/dev/null 2>&1; then
  red "docker is not responding. Run ./scripts/install-brew.sh, then: open -a OrbStack"
  exit 1
fi

green "==> config: scale=$SCALE churn=$CHURN mode=$MODE cpus=$ENGINE_CPUS mem=$ENGINE_MEM"

green "==> building images"
# ALL services, not just gen. Every service builds its own image from the same context,
# and `compose up` happily reuses a stale one -- so building only the generator meant an
# edit to src/incremental.py could sit unbuilt while the run measured the previous
# version and reported it as a result. That happened: a duplicate-row fix was live in the
# working tree and absent from the image the engines actually ran.
docker compose build --quiet gen vanilla incremental verify

STAMP="data/lake/.stamp"
if [[ $SKIP_GEN -eq 0 ]] && [[ ! -f $STAMP || "$(cat $STAMP 2>/dev/null)" != "$SCALE:$CHURN" ]]; then
  green "==> generating lake (scale=$SCALE churn=$CHURN)"
  rm -rf data/lake && mkdir -p data/lake
  docker compose run --rm gen
  echo "$SCALE:$CHURN" > $STAMP
else
  green "==> reusing existing lake ($(cat $STAMP 2>/dev/null))"
fi

green "==> clearing previous run"
rm -rf results data/out_vanilla data/out_incremental
mkdir -p results data/out_vanilla data/out_incremental

# The dashboard has to know when to stop waiting. Read out of config.py rather than
# written down here, so the tick count has exactly one definition: a second copy would
# drift the moment a preset changed, and the dashboard would exit a cycle early.
TICKS=$(python3 - "$SCALE" <<'PY'
import sys, re
src = open("src/config.py").read()
m = re.search(rf'"{re.escape(sys.argv[1])}": Scale\((.*?)\),\n', src, re.S)
print(re.search(r"ticks=(\d+)", m.group(1)).group(1) if m else 20)
PY
)

if [[ $MODE == parallel ]]; then
  green "==> starting both engines (parallel, identical caps)"
  docker compose up -d vanilla incremental
else
  green "==> starting vanilla"
  docker compose up -d vanilla
fi

green "==> live dashboard (ctrl-c to detach; containers keep running)"
set +e
uv run python -m dashboard.live --results ./results --expect-ticks "$TICKS"
set -e

if [[ $MODE == sequential ]]; then
  docker compose wait vanilla >/dev/null 2>&1 || true
  green "==> starting incremental"
  docker compose up -d incremental
  set +e
  uv run python -m dashboard.live --results ./results --expect-ticks "$TICKS"
  set -e
fi

green "==> waiting for engines to exit"
docker compose wait vanilla incremental >/dev/null 2>&1 || true

green "==> correctness gate"
GATE_OK=1
if docker compose run --rm verify; then
  green "gate passed: the two engines produced identical fact tables"
else
  GATE_OK=0
  red "GATE FAILED: the fact tables differ. Performance numbers from this run mean nothing."
fi

green "==> report"
uv run python -m dashboard.report --results ./results

# The gate has to actually fail the run. The README calls it the thing that makes every
# other number meaningful, so a red message followed by exit 0 would be theatre: CI would
# go green and the timings would get quoted anyway. The report still prints first, because
# when the gate fails the per-cycle numbers are the evidence you need to debug it.
if [[ $GATE_OK -eq 0 ]]; then
  red "run failed: correctness gate did not pass"
  exit 1
fi
