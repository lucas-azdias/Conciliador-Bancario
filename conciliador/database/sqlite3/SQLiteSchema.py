import typeguard
import typing

from .. import Schema


class SQLiteSchema(Schema.Schema):

    @property
    @typeguard.typechecked
    def id_datatype(self) -> str:
        return "INTEGER PRIMARY KEY AUTOINCREMENT"


    @property
    @typeguard.typechecked
    def datatypes(self) -> typing.Tuple[str, ...]:
        return (
            "TEXT",
            "NUMERIC",
            "INTEGER",
            "REAL",
            "BLOB",
        )


    @property
    @typeguard.typechecked
    def default_tables(self) -> typing.Tuple[str, ...]:
        return (
            "sqlite_sequence",
        )