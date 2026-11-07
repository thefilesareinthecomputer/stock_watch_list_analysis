"""Existence checks for optional warehouse tables and columns.

Many readers degrade gracefully when a tier was not built (no fundamentals,
no calls yet, an older fixture); this is the one place that asks.
"""


def has_table(con, table):
    return bool(con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table]).fetchone()[0])


def has_column(con, table, column):
    return bool(con.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_name = ? AND column_name = ?",
        [table, column]).fetchone()[0])
