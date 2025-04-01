from __future__ import annotations
import re
from typing import Literal, Tuple


DB_TABLE_NAME_PATTERN = re.compile(
    r"""
    (?:
        CREATE(?:\s+OR\s+REPLACE)?\s+(?:TEMPORARY\s+)?(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?  # DDL
        |ALTER\s+(?:TABLE|VIEW)\s+  # DDL
        |DROP\s+(?:TEMPORARY\s+)?(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?  # DDL
        |TRUNCATE\s+(?:TABLE\s+)?  # DDL
        |RENAME\s+TABLE\s+  # DDL
        |INSERT\s+(?:IGNORE\s+)?(?:INTO\s+)?  # DML
        |REPLACE\s+(?:INTO\s+)?  # DML
        |UPDATE\s+(?:IGNORE\s+)?(?:\s+|\s+(?:LOW_PRIORITY)\s+)?  # DML
        |DELETE\s+(?:IGNORE\s+)?(?:FROM\s+)?  # DML
    )
    \s*  # Match whitespace
    (?:([`"]?[a-zA-Z_][a-zA-Z0-9_$]*[`"]?)\.)?  # Optional database/schema
    (?:  # Table name group
        [`"]([a-zA-Z_][a-zA-Z0-9_\s$]*)[`"]  # with quotes (spaces allowed)
        |([a-zA-Z_][a-zA-Z0-9_$]*)  # single word table name
    )
    (?=[\s;)]|$)  # Lookahead
    """,
    re.IGNORECASE | re.VERBOSE,
)


def parse_db_table_name_from_query(query: str) -> Tuple[str | None, str | None]:
    """Parse the database and table name from a query."""

    match = DB_TABLE_NAME_PATTERN.search(query)
    return (match.group(1), match.group(2)) if match else (None, None)


DDL_QUERY_PATTERN = re.compile(
    r"^\b(CREATE|ALTER|DROP|TRUNCATE|RENAME|GRANT|REVOKE|LOCK|UNLOCK)\b",
    re.IGNORECASE,
)

TCL_QUERY_PATTERN = re.compile(r"^(\bCOMMIT\b|\bROLLBACK\b|)", re.IGNORECASE)


def type_of_query(
    query: str,
) -> Literal["INSERT", "UPDATE", "DELETE", "REPLACE", "SELECT", "DDL", "TCL"] | None:
    if len(query) < 6:
        return None

    query_type = query[:6].upper()
    if query_type in ["INSERT", "UPDATE", "DELETE", "SELECT"]:
        return query_type

    if query[:7].upper() == "REPLACE":
        return "REPLACE"

    query_type = query[:10]
    if bool(DDL_QUERY_PATTERN.search(query_type)):
        return "DDL"
    elif bool(TCL_QUERY_PATTERN.search(query_type)):
        return "TCL"

    return None
