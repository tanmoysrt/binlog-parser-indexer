from __future__ import annotations
import re
from typing import Literal, Tuple
from sql_metadata import Parser


def parse_db_table_name_from_query(query: str) -> Tuple[str | None, str | None]:
    """Parse the database and table name from a query."""

    try:
        tables = Parser(query).tables
        if len(tables) == 0:
            return (None, None)

        table = tables[0]
        splitted = table.split(".")
        if len(splitted) == 1:
            return (None, table)
        elif len(splitted) == 2:
            return (splitted[0], splitted[1])
    except Exception as e:
        print(e)
        return (None, None)


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
