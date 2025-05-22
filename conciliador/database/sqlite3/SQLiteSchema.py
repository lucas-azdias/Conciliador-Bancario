import re
import typeguard
import typing

from .. import Schema


class SQLiteSchema(Schema.Schema):

    @property
    @typeguard.typechecked
    def id_datatype(self) -> re.Pattern[str]:
        return re.compile("INTEGER PRIMARY KEY AUTOINCREMENT")


    @property
    @typeguard.typechecked
    def datatypes(self) -> typing.Tuple[re.Pattern[str], ...]:
        return (
            re.compile(string) for string in (
                "TEXT",
                "NUMERIC",
                "INTEGER",
                "REAL",
                "BLOB",
            )
        )


    @property
    @typeguard.typechecked
    def default_tables(self) -> typing.Tuple[str, ...]:
        return (
            "sqlite_sequence",
        )