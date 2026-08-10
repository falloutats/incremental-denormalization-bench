"""The Fact dependency graph -- the first of two places this demo is allowed to be clever.

From the blog: "Facts are modeled as directed graphs where nodes are tables and edges
are join predicates." Everything the incremental engine does is a traversal of this
structure:

  * forward  (root -> leaves) builds a denormalised row from a changed payment
  * backward (leaf -> root)   recovers which payments a changed offer affects

The backward direction is the whole trick. When a dimension row changes you do not know
which fact rows it touches, and you must not scan the fact to find out. You walk back up
the edges through the *secondary indexes*, which are small enough to join cheaply.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Table:
    name: str
    pk: str
    partition_col: str | None  # None => small dimension, not partitioned on the lake


@dataclass(frozen=True)
class Edge:
    """A join predicate: parent.parent_col == child.child_col."""

    parent: str
    child: str
    parent_col: str
    child_col: str

    def predicate(self) -> str:
        return f"{self.parent}.{self.parent_col} = {self.child}.{self.child_col}"


class FactGraph:
    def __init__(self, root: str, tables: dict[str, Table], edges: list[Edge]):
        self.root = root
        self.tables = tables
        self.edges = edges

    # --- structure ----------------------------------------------------------

    def children_of(self, table: str) -> list[Edge]:
        return [e for e in self.edges if e.parent == table]

    def parent_edge_of(self, table: str) -> Edge | None:
        """The single edge by which `table` hangs off its parent. None for the root."""
        matches = [e for e in self.edges if e.child == table]
        if not matches:
            return None
        if len(matches) > 1:
            raise ValueError(f"{table} has multiple parents; this demo assumes a tree")
        return matches[0]

    def forward_order(self) -> list[Edge]:
        """Edges in level order from the root -- the order to apply joins when building
        a fact row. Breadth-first so a parent is always joined before its children."""
        order: list[Edge] = []
        frontier = [self.root]
        seen = {self.root}
        while frontier:
            nxt = []
            for table in frontier:
                for edge in self.children_of(table):
                    if edge.child in seen:
                        continue
                    order.append(edge)
                    seen.add(edge.child)
                    nxt.append(edge.child)
            frontier = nxt
        return order

    def path_to_root(self, table: str) -> list[Edge]:
        """Edges walked from `table` up to the root, nearest-first.

        This is the back-traversal from the blog's secondary flow. For offers it yields
        [discounts->offers, payments->discounts], meaning: join changed offers to the
        discounts index on offer_id, then join that to the payments index on payment_id,
        and you have the affected payment ids plus the partitions they live in.
        """
        path: list[Edge] = []
        node = table
        while node != self.root:
            edge = self.parent_edge_of(node)
            if edge is None:
                raise ValueError(f"{table} is not connected to root {self.root}")
            path.append(edge)
            node = edge.parent
        return path

    # --- secondary index spec ----------------------------------------------

    def index_columns(self, table: str) -> list[str]:
        """Columns the secondary index for `table` must carry.

        Per the blog: "a separate small index table per entity stores only join columns
        and created_date partition values ... significantly smaller and easier to
        maintain than the original source table."

        That is: the primary key, every column of this table that participates in a join,
        and the partition value. No payload columns -- adding those would recreate the
        source table and defeat the purpose.
        """
        t = self.tables[table]
        cols = {t.pk}
        for e in self.edges:
            if e.parent == table:
                cols.add(e.parent_col)
            if e.child == table:
                cols.add(e.child_col)
        if t.partition_col:
            cols.add(t.partition_col)
        # Stable order so parquet schemas are reproducible across runs.
        return sorted(cols)

    def indexed_tables(self) -> list[str]:
        """Every table that lives in partitions needs an index -- "a separate small index
        table per entity", as the blog puts it.

        Two distinct jobs, and both are load-bearing:
          forward   given a changed payment's card_id, which partition of cards holds it?
                    Cards are partitioned by their own created_date, which has nothing to
                    do with the payment's, so without the index this is a full scan.
          backward  given a changed offer and nothing else, which payments are affected
                    and where do they live? Without indexes this is a full scan of
                    discounts followed by a full scan of payments.

        Unpartitioned dimensions (offers) are read whole -- they are tiny -- so they
        need no index.
        """
        return sorted(name for name, t in self.tables.items() if t.partition_col)


# --- the concrete graph for payments_fact -----------------------------------
# Five tables instead of Razorpay's 10-30, but with the same shape: two lookup
# dimensions off the root, one child collection, and one dimension two hops out.
# The 2-hop edge is what makes back-traversal non-trivial; a star schema would not.

TABLES = {
    "payments": Table("payments", pk="id", partition_col="created_date"),
    "orders": Table("orders", pk="id", partition_col="created_date"),
    "cards": Table("cards", pk="id", partition_col="created_date"),
    "discounts": Table("discounts", pk="id", partition_col="created_date"),
    "offers": Table("offers", pk="id", partition_col=None),
}

EDGES = [
    Edge("payments", "orders", parent_col="order_id", child_col="id"),
    Edge("payments", "cards", parent_col="card_id", child_col="id"),
    Edge("payments", "discounts", parent_col="id", child_col="payment_id"),
    Edge("discounts", "offers", parent_col="offer_id", child_col="id"),
]

FACT_GRAPH = FactGraph(root="payments", tables=TABLES, edges=EDGES)

# Columns each table contributes to the wide fact row, keyed by table.
# The root contributes its own columns; everything else is prefixed to avoid collisions.
FACT_COLUMNS = {
    "payments": ["id", "merchant_id", "amount", "status", "created_date", "updated_at"],
    "orders": ["receipt"],
    "cards": ["network", "last4"],
    "discounts": ["amount"],
    "offers": ["name", "percent"],
}


def fact_column_names(graph: FactGraph | None = None) -> list[str]:
    """The exact column list of the fact table, in a fixed order.

    Both engines end their build with a select on this, which is what lets the
    correctness gate compare them directly instead of arguing about column order.
    """
    g = graph or FACT_GRAPH
    cols = list(FACT_COLUMNS[g.root])
    for edge in g.forward_order():
        cols.extend(f"{edge.child}_{c}" for c in FACT_COLUMNS[edge.child])
    return cols


def join_keys_needed(table: str, graph: FactGraph | None = None) -> list[str]:
    """Columns of `table` that its own outgoing edges join on, and which therefore have
    to survive the join even though they are not fact payload."""
    g = graph or FACT_GRAPH
    return [e.parent_col for e in g.children_of(table)]
