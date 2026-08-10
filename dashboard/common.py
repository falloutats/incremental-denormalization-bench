"""The handful of things the live dashboard and the final report must agree on.

Both read the same JSONL streams and print the same numbers to the same humans, so the
engine list and the byte formatting live in one place rather than being copied. Everything
else -- layout, chart selection, which costs to call out -- is genuinely different between
a thing that updates four times a second and a thing that prints once, and is kept apart.
"""

from __future__ import annotations

# Order matters: it is the order engines appear in tables and legends.
ENGINES = ("vanilla", "incremental")

# One colour per engine, used for both rich styles and plotext series, so the full refresh
# is the same red in every chart on every screen.
COLORS = {"vanilla": "red", "incremental": "green"}


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:,.0f}{unit}" if unit == "B" else f"{n:,.1f}{unit}"
        n /= 1024
    return f"{n:,.1f}PB"
