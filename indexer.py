import contextlib
import os
from typing import Literal

from models import QueryModel, BinlogModel
from parser import BinlogParser


class BinlogIndexer:
    def __init__(self, base_path: str = "/var/lib/mysql"):
        self.base_path = base_path

    def add(self, binlog_name: str):
        path = os.path.join(self.base_path, binlog_name)
        if BinlogModel.get_or_none(BinlogModel.name == binlog_name):
            return

        # check if file exists
        if not os.path.exists(path):
            raise FileNotFoundError(f"File {path} does not exist")

        try:
            file = open(path, "rb")
            data = file.read()

            parser = BinlogParser(data)
            queries = list(parser.parse_queries())

            rows = []
            for q in queries:
                for source in q.sources:
                    rows.append(
                        [
                            q.timestamp,
                            source[0],
                            source[1],
                            q.type,
                            q.query,
                            q.is_truncated,
                            q.query_start,
                            q.query_end,
                            q.event_start,
                            q.event_length,
                            q.related_events_end_pos,
                            binlog_name,
                        ]
                    )

            QueryModel.insert_many(
                rows,
                fields=[
                    QueryModel.timestamp,
                    QueryModel.database,
                    QueryModel.table,
                    QueryModel.type,
                    QueryModel.query,
                    QueryModel.is_query_truncated,
                    QueryModel.query_start,
                    QueryModel.query_end,
                    QueryModel.event_start,
                    QueryModel.event_length,
                    QueryModel.related_events_end_pos,
                    QueryModel.binlog_name,
                ],
            ).execute()

            BinlogModel.create(name=binlog_name)
        except Exception:
            with contextlib.suppress(Exception):
                file.close()
            raise

    def remove(self, binlog_name: str):
        BinlogModel.delete().where(BinlogModel.name == binlog_name).execute()
        QueryModel.delete().where(QueryModel.binlog_name == binlog_name).execute()

    def search(
        self,
        database: str,
        table: str | None,
        query_type: Literal["INSERT", "UPDATE", "DELETE", "REPLACE", "SELECT", "DDL"]
        | None,
        start_timestamp: int,
        end_timestamp: int,
        page_no: int = 1,
    ) -> tuple[list[dict], int]:
        """
        Returns a tuple
         - list of dicts
         - total number of records
         - total number of pages
         - current page number
        """
        pass

    def _count(
        self,
        database: str,
        table: str | None,
        query_type: str | None,
        start_timestamp: int,
        end_timestamp: int,
    ):
        pass
