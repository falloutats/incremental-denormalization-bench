#!/usr/bin/env bash
# System-level dependencies. Homebrew ONLY — no Python, no uv, no pip.
# Python deps live in scripts/setup-python.sh. Keep it that way.
#
# Installs:
#   orbstack    container runtime (Docker-compatible, needed for the CPU/RAM-capped comparison)
#   openjdk@17  PySpark 3.5+ requires Java 17+; macOS here ships only Java 16 (x86_64)
#
# Idempotent: re-running is a no-op for anything already present.

set -euo pipefail

green() { printf '\033[32m%s\033[0m\n' "$1"; }
yellow() { printf '\033[33m%s\033[0m\n' "$1"; }
red() { printf '\033[31m%s\033[0m\n' "$1"; }

if ! command -v brew >/dev/null 2>&1; then
  red "Homebrew not found. Install it first: https://brew.sh"
  exit 1
fi

green "==> Homebrew $(brew --version | head -1)"

# --- openjdk@17 -------------------------------------------------------------
if brew list --formula openjdk@17 >/dev/null 2>&1; then
  yellow "openjdk@17 already installed, skipping"
else
  green "==> Installing openjdk@17 (PySpark needs Java 17+)"
  brew install openjdk@17
fi

JAVA_17_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"

# --- orbstack ---------------------------------------------------------------
if brew list --cask orbstack >/dev/null 2>&1; then
  yellow "orbstack already installed, skipping"
elif command -v docker >/dev/null 2>&1; then
  yellow "a 'docker' command already exists, skipping orbstack install"
else
  green "==> Installing orbstack (container runtime)"
  brew install --cask orbstack
fi

# --- stale Docker Desktop shims ---------------------------------------------
# A previous Docker Desktop install can leave dangling symlinks in /usr/local/bin.
# If /usr/local/bin precedes ~/.orbstack/bin on PATH they shadow OrbStack's working
# shims and every docker command dies with "command not found".
if [ -L /usr/local/bin/docker ] && [ ! -e /usr/local/bin/docker ]; then
  yellow "Note: /usr/local/bin/docker is a dangling symlink from an old Docker Desktop install."
  yellow "      env.sh puts ~/.orbstack/bin first on PATH, so it is shadowed harmlessly."
  yellow "      To clean up permanently: sudo rm /usr/local/bin/docker*"
fi

# --- start OrbStack ---------------------------------------------------------
if ! pgrep -qf "OrbStack.app/Contents/MacOS/OrbStack"; then
  green "==> Starting OrbStack"
  open -a OrbStack
  for _ in $(seq 1 30); do
    "$HOME/.orbstack/bin/docker" info >/dev/null 2>&1 && break
    sleep 2
  done
fi

# --- report -----------------------------------------------------------------
cat <<EOF

$(green "Done.")

All environment setup lives in env.sh. Source it in any shell that runs the demo:

    source ./env.sh

It sets JAVA_HOME (Java 17), SPARK_LOCAL_IP, and puts ~/.orbstack/bin first on PATH.

Verify:

    java -version      # expect 17.x
    docker info        # expect a running engine

Next: ./scripts/setup-python.sh
EOF
