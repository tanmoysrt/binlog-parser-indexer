import struct
from typing import Literal
from utils import (
    parse_db_table_name_from_query,
    type_of_query,
)

MagicNumber = b"\xfe\x62\x69\x6e"
FormatDescriptionEvent = 0x0F  # https://mariadb.com/kb/en/format_description_event/
QueryEvent = 0x02  # https://mariadb.com/kb/en/query_event/
AnnotateRowsEvent = 0xA0  # https://mariadb.com/kb/en/annotate_rows_event/
TableMapEvent = 0x13  # https://mariadb.com/kb/en/table_map_event/
# https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
WriteRowsV1Event = 0x17
UpdateRowsV1Event = 0x18
DeleteRowsV1Event = 0x19


class EventHeader:
    # https://mariadb.com/kb/en/2-binlog-event-header/

    def __init__(self, data: bytes, cur_pos: int):
        self.position = cur_pos
        unpacked = struct.unpack("<IB4xII2x", data)
        self.timestamp = unpacked[0]
        self.event_type = unpacked[1]
        self.event_length = unpacked[2]  # header (19) + data (variable)
        self.next_event_position = unpacked[3]

    def __repr__(self):
        return (
            "<Header Start>\n"
            f"  Position : {self.position}\n"
            f"  Timestamp : {self.timestamp}\n"
            f"  Event Type : {self.event_type}\n"
            f"  Event Length : {self.event_length}\n"
            f"  Next Event Position : {self.next_event_position}\n"
            "</Header End>\n"
        )


class TableMapEventData:
    # https://mariadb.com/kb/en/table_map_event/

    def __init__(self, data: bytes):
        self.table_id = struct.unpack("<Q", data[:6] + b"\x00\x00")[0]
        db_name_len = struct.unpack("!B", data[8:9])[0]
        self.db = data[9 : 9 + db_name_len].decode("latin-1")
        # [NOTE]: db_name ended with a null byte, so skip it
        table_name_len = struct.unpack("!B", data[10 + db_name_len : 11 + db_name_len])[
            0
        ]
        self.table = data[11 + db_name_len : 11 + db_name_len + table_name_len].decode(
            "latin-1"
        )

    def __repr__(self):
        return (
            f"<TableMapEventData>\n"
            f"  Table ID : {self.table_id}\n"
            f"  Database : {self.db}\n"
            f"  Table : {self.table}\n"
            "</TableMapEventData>\n"
        )


class AnnotateRowsEventData:
    # https://mariadb.com/kb/en/annotate_rows_event/

    def __init__(self, data: bytes):
        self._query_start = 0
        self._query_end = len(data) - 4
        self.query = data[self._query_start : self._query_end].decode(
            "latin-1"
        )  # last 4 bytes are the checksum
        self.query_type: (
            Literal["INSERT", "UPDATE", "DELETE", "REPLACE", "SELECT", "DDL", "TCL"]
            | None
        ) = type_of_query(self.query)
        # Extract db and table from query
        db_name_in_query, table_name = parse_db_table_name_from_query(self.query)
        self.db = db_name_in_query
        self.table = table_name

    def __repr__(self):
        return (
            f"<AnnotateRowsData>\n"
            f"  Database : {self.db}\n"
            f"  Table : {self.table}\n"
            f"  Query : {self.query}\n"
            f"</AnnotateRowsData>\n"
        )


class QueryEventData:
    # https://mariadb.com/kb/en/query_event/

    def __init__(self, data: bytes):
        db_name_len = struct.unpack("<B", data[8:9])[0]
        status_vars_len = struct.unpack("<H", data[11:13])[0]
        db_name_start = 13 + status_vars_len

        self.db = data[db_name_start : db_name_start + db_name_len].decode("latin-1")
        self._query_start = (
            db_name_start + db_name_len + 1
        )  # skip 1 byte for null terminator \x00
        self._query_end = len(data) - 4
        self.query = data[self._query_start : self._query_end].decode(
            "latin-1"
        )  # last 4 bytes are CRC32

        db_name_in_query, table_name = parse_db_table_name_from_query(self.query)
        if db_name_in_query:
            # If database name is defined in query, use that
            self.db = db_name_in_query

        self.table = table_name
        self.query_type: (
            Literal["INSERT", "UPDATE", "DELETE", "REPLACE", "SELECT", "DDL", "TCL"]
            | None
        ) = type_of_query(self.query)

    def __repr__(self):
        return (
            f"<QueryEventData>\n"
            f"  Database : {self.db}\n"
            f"  Table : {self.table}\n"
            f"  Type : {self.query_type}\n"
            f"  Query : {self.query}\n"
            "</QueryEventData>\n"
        )


class Query:
    def __init__(
        self,
        sources: list[tuple[str, str]],
        timestamp: int,
        type: Literal["INSERT", "UPDATE", "DELETE", "REPLACE", "SELECT", "DDL", "TCL"],
        query: str,
        event_start: int,  # header -> fixed size 19 bytes | header_start -> event_start
        event_length: int,  # event_length -> (header + data) | data_start -> event_start + 19 | data_end -> event_start + event_length
        query_start: int,
        query_end: int,
        related_events_end_pos: int,
    ):
        self.sources = sources  # list of tuples (database, table)
        self.timestamp = timestamp
        self.type = type
        self.query = query
        self.is_truncated = False
        # if query is more than 500 characters, truncate it
        if len(self.query) > 500:
            self.query = self.query[:200] + "..." + self.query[-300:]
            self.is_truncated = True
        self.query_start = query_start
        self.query_end = query_end
        self.event_start = event_start
        self.event_length = event_length
        self.related_events_end_pos = related_events_end_pos

    def __repr__(self):
        return (
            f"<Query>\n"
            f"\tTimestamp : {self.timestamp}\n"
            f"\tSources : {self.sources}\n"
            f"\tType : {self.type}\n"
            f"\tQuery : {self.query}\n"
            f"\tQuery Start : {self.query_start}\n"
            f"\tQuery End : {self.query_end}\n"
            f"\tEvent Start : {self.event_start}\n"
            f"\tEvent Length : {self.event_length}\n"
            f"\tRelated Events End Position : {self.related_events_end_pos}\n"
            "</Query>\n"
        )


class BinlogParser:
    ALLOWED_EVENT_TYPES = [
        TableMapEvent,
        QueryEvent,
        AnnotateRowsEvent,
        WriteRowsV1Event,
        UpdateRowsV1Event,
        DeleteRowsV1Event,
    ]

    def __init__(self, data):
        self.data = data
        if len(self.data) < 4 or self.data[:4] != b"\xfe\x62\x69\x6e":
            raise ValueError("Invalid binlog file provided")
        self.cur_header_index = 0
        self.table_map: dict[int, tuple[str, str]] = {}

        # Parse the headers
        self.headers = self._parse_headers()

    def parse_queries(self):
        while self.current_header:
            query = self._parse_event(self.current_header)
            if query:
                yield query

    @property
    def current_header(self) -> EventHeader | None:
        if self.cur_header_index < len(self.headers):
            return self.headers[self.cur_header_index]
        return None

    @property
    def next_header(self) -> EventHeader | None:
        if self.cur_header_index + 1 < len(self.headers):
            return self.headers[self.cur_header_index + 1]
        return None

    def move_to_next_header(self):
        self.cur_header_index += 1

    def _parse_headers(self) -> list[EventHeader]:
        """Parse all binlog event headers from the data."""
        headers = []
        position = 4
        data_len = len(self.data)

        # header size is 19 bytes
        while position + 19 <= data_len:
            try:
                header = EventHeader(self.data[position : position + 19], position)
            except Exception as e:
                raise ValueError(
                    f"Failed to parse header at position {position}: {str(e)}"
                )

            headers.append(header)
            if header.next_event_position == 0:
                break

            position = header.next_event_position

        return headers

    def _parse_event(self, header: EventHeader) -> Query | None:
        if header.event_type not in self.ALLOWED_EVENT_TYPES:
            self.move_to_next_header()
            return None

        result = None
        move_to_next_header = True

        if header.event_type == QueryEvent:
            event = QueryEventData(self._get_body(header))
            if event.query_type != "TCL":
                # No need to log TCL queries
                result = Query(
                    database=event.db,
                    table=event.table,
                    timestamp=header.timestamp,
                    type=event.query_type,
                    event_start=header.position,
                    event_length=header.event_length,
                    query=event.query,
                    query_start=event._query_start + header.position + 19,
                    query_end=event._query_end + header.position + 19,
                    related_events_end_pos=header.position + header.event_length,
                )

        elif header.event_type == AnnotateRowsEvent:
            annotateRowsData = AnnotateRowsEventData(self._get_body(header))
            # read all the table map events
            current_table_map_ids = []
            while self.next_header and self.next_header.event_type == TableMapEvent:
                table_map_event_data = TableMapEventData(
                    self._get_body(self.next_header)
                )
                self.table_map[table_map_event_data.table_id] = (
                    table_map_event_data.db,
                    table_map_event_data.table,
                )
                current_table_map_ids.append(table_map_event_data.table_id)
                self.move_to_next_header()
                move_to_next_header = False

            query_type = None
            if self.next_header:
                if self.next_header.event_type == WriteRowsV1Event:
                    query_type = "INSERT"
                elif self.next_header.event_type == UpdateRowsV1Event:
                    query_type = "UPDATE"
                elif self.next_header.event_type == DeleteRowsV1Event:
                    query_type = "DELETE"

            if query_type is None:
                query_type = annotateRowsData.query_type

            # keep reading all the rows events
            while self.next_header and self.next_header.event_type in [
                WriteRowsV1Event,
                UpdateRowsV1Event,
                DeleteRowsV1Event,
            ]:
                self.move_to_next_header()
                move_to_next_header = False

            if query_type:
                sources = []
                for table_id in current_table_map_ids:
                    sources.append(
                        (self.table_map[table_id][0], self.table_map[table_id][1])
                    )
                if len(sources) == 0:
                    sources = [(annotateRowsData.db, annotateRowsData.table)]

                result = Query(
                    sources=sources,
                    timestamp=header.timestamp,
                    type=query_type,
                    event_start=header.position,
                    event_length=header.event_length,
                    query=annotateRowsData.query,
                    query_start=annotateRowsData._query_start + header.position + 19,
                    query_end=annotateRowsData._query_end + header.position + 19,
                    related_events_end_pos=(
                        self.current_header.position + self.current_header.event_length
                        if self.current_header is not None
                        else header.position + header.event_length
                    ),
                )

        if move_to_next_header:
            self.move_to_next_header()

        return result

    def _get_body(self, header: EventHeader) -> bytes:
        return self.data[header.position + 19 : header.position + header.event_length]
