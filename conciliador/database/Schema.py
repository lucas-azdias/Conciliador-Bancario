import collections
import copy
import json
import pathlib
import typeguard
import typing


DATATYPES = (
    "TEXT",
    "NUMERIC",
    "INTEGER",
    "REAL",
    "BLOB",
)


class Schema():

    @typeguard.typechecked
    def __init__(self, path: typing.Optional[pathlib.Path] = None):
        self.__tables: typing.Dict[str, typing.Dict[str, str]] = collections.defaultdict(dict)
        if path:
            self.import_schema(path)


    @typeguard.typechecked
    def copy(self) -> typing.Self:
        return copy.deepcopy(self)


    @typeguard.typechecked
    def import_schema(
            self,
            path: pathlib.Path
        ):
        # E.g.:
        # {
        #     "table": {
        #         "column": "type"
        #     }
        # }
        with open(path, "r") as file:
            schema = json.load(file)
            file.close()

        for table_name, columns in schema.items():
            self.add_table(table_name, columns)


    @typeguard.typechecked
    def add_table(
            self,
            table_name: str,
            columns: typing.Dict[str, str]
        ) -> None:
        if not table_name:
            raise Exception("Missing table name to create table.")

        if not columns:
            raise Exception("Missing columns to create table.")

        if table_name in self.__tables:
            raise Exception(f"Table name \"{table_name}\" already exists on tables.")

        if not all(type.upper() in DATATYPES for type in columns.values()):
            raise Exception("Type not found on datatypes.")

        for column_name, column_type in columns.items():
            self.__tables[table_name][column_name] = column_type


    @typeguard.typechecked
    def remove_table(
            self,
            table_name: str
        ) -> None:
        if not table_name in self.__tables:
            raise Exception(f"Table name \"{table_name}\" not found on tables.")

        self.__tables.pop(table_name)


    @typeguard.typechecked
    def add_column(
            self,
            table_name: str,
            column_name: str,
            column_type: str
        ) -> None:
        if not table_name in self.__tables:
            raise Exception(f"Table name \"{table_name}\" not found on tables.")

        if column_name in self.__tables[table_name]:
            raise Exception(f"Column name \"{column_name}\" already exists on table.")

        self.__tables[table_name][column_name] = column_type


    @typeguard.typechecked
    def remove_column(
            self,
            table_name: str,
            column_name: str
        ) -> None:
        if not table_name in self.__tables:
            raise Exception(f"Table name \"{table_name}\" not found on tables.")

        if not column_name in self.__tables[table_name]:
            raise Exception(f"Column name \"{column_name}\" not found on table.")

        self.__tables[table_name].pop(column_name)


    @typeguard.typechecked
    def tables(self) -> typing.Tuple[str, ...]:
        return tuple(self.__tables.keys())


    @typeguard.typechecked
    def columns(
            self,
            table_name: str
        ) -> typing.Dict[str, str]:
        if not table_name in self.__tables:
            raise Exception(f"Table name \"{table_name}\" not found on tables.")

        return dict(self.__tables[table_name].items())