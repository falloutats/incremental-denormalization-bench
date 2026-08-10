#!/usr/bin/env bash
# Python dependencies. uv ONLY — no brew calls in this file.
# System deps (Java, OrbStack) live in scripts/install-brew.sh.
#
# Idempotent: uv sync converges to pyproject.toml; re-running is cheap.

set -euo pipefail

green() { printf '\033[32m%s\033[0m\n' "$1"; }
red() { printf '\033[31m%s\033[0m\n' "$1"; }

cd "$(dirname "$0")/.."

if ! command -v uv >/dev/null 2>&1; then
  red "uv not found. Install it: curl -LsSf https://astral.sh/uv/install.sh | sh"
  exit 1
fi

green "==> uv $(uv --version)"
green "==> Syncing environment from pyproject.toml"
uv sync

cat <<'EOF'

Done.

Verify:

    uv run python -c "import pyspark, pyarrow, numpy, rich, plotext; print(pyspark.__version__)"

That import needs JAVA_HOME pointing at Java 17 (see scripts/install-brew.sh).
EOF
