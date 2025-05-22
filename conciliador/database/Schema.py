import abc
import collections
import copy
import json
import pathlib
import re
import typeguard
import typing


class Schema(abc.ABC):

    @typeguard.typechecked
    def __init__(
        self,
        id_column: str,
        path: typing.Optional[pathlib.Path] = None
    ) -> None:
        self.__id_column = id_column

        self.__tables: typing.Dict[str, typing.Dict[str, str]] = collections.defaultdict(dict)
        if path:
            self.import_schema(path)


    @property
    @typeguard.typechecked
    def id_column(self) -> str:
        return self.__id_column


    @property
    @abc.abstractmethod
    def id_datatype(self) -> re.Pattern[str]:
        pass


    @property
    @abc.abstractmethod
    def datatypes(self) -> typing.Tuple[re.Pattern[str], ...]:
        pass


    @property
    @abc.abstractmethod
    def default_tables(self) -> typing.Tuple[str, ...]:
        pass


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
            columns[self.__id_column] = self.id_datatype
            self.add_table(table_name, columns)


    @typeguard.typechecked
    def contains_datatype(
        self,
        datatype: str
    ) -> bool:
        return any(
            pattern.match(datatype) for pattern in self.datatypes
        ) or self.id_datatype.match(datatype)


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

        if not all(self.contains_datatype(type) for type in columns.values()):
            raise Exception("Type did not match anyone on datatypes.")

        # Garantee id column at start
        self.__tables[table_name][self.__id_column] = None

        # Add all given columns
        for column_name, column_type in columns.items():
            self.__tables[table_name][column_name] = column_type

        # Overwrite any changes to id column
        self.__tables[table_name][self.__id_column] = self.id_datatype


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

        if not self.contains_datatype(column_type):
            raise Exception("Type did not match anyone on datatypes.")

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

        if column_name == self.__id_column:
            raise Exception(f"Column name \"{column_name}\" is conflicting with \"id\" column.")

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