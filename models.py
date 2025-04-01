from peewee import (
    CharField,
    Model,
    SqliteDatabase,
    BigIntegerField,
    BooleanField,
)

binlog_database = SqliteDatabase(
    "binlog.db",
    timeout=15,
    pragmas={
        "journal_mode": "wal",
        "synchronous": "normal",
        "mmap_size": 1048576,  # 1MB
        "page_size": 4096,  # 4KB
    },
)


class QueryModel(Model):
    timestamp = BigIntegerField(index=True)

    database = CharField(max_length=70, index=True)
    table = CharField(max_length=70, index=True)

    type = CharField(
        choices=[
            (0, "INSERT"),
            (1, "UPDATE"),
            (2, "DELETE"),
            (3, "REPLACE"),
            (4, "SELECT"),
            (5, "DDL"),
        ],
        index=True,
    )
    query = CharField(max_length=600)
    is_query_truncated = BooleanField()

    query_start = BigIntegerField()
    query_end = BigIntegerField()
    event_start = BigIntegerField()
    event_length = BigIntegerField()
    related_events_end_pos = BigIntegerField()

    binlog_name = CharField(max_length=60)

    class Meta:
        database = binlog_database


class BinlogModel(Model):
    name = CharField(max_length=60, index=True)

    class Meta:
        database = binlog_database


# binlog_database.create_tables([QueryModel, BinlogModel])
