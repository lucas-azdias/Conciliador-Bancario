import abc
import pathlib
import polars
import typeguard
import typing

from . import Schema


class Database(abc.ABC):

    @typeguard.typechecked
    def __init__(
            self,
            schema: Schema.Schema,
            database_path: typing.Optional[pathlib.Path] = None,
            can_load_schema: bool = False, # Enables add tables/columns from schema to the database
            can_purge: bool = False # Enables purge tables/columns from database based on schema
        ) -> None:
        self._schema: Schema.Schema = schema
        self._database_path: typing.Optional[pathlib.Path] = database_path.absolute() if database_path else None

        # Create database storage file if not created 
        self._connect()
        self._close()

        # Validates schema
        self._validate_schema(can_load_schema = can_load_schema, can_purge = can_purge)


    @typeguard.typechecked
    def create_table(
            self,
            table_name: str,
            columns: typing.Dict[str, str]
        ) -> None:
        if table_name in self._schema.tables():
            raise Exception("Table name already exists on schema tables.")

        try:
            self._connect()
            self._create_table(table_name, columns)

        except Exception as e:
            raise Exception(f"Failed to create table: {e}")

        else:
            self._schema.add_table(table_name, columns)

        finally:
            self._close()


    @typeguard.typechecked
    def drop_table(
            self,
            table_name: str
        ) -> None:
        if not table_name in self._schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self._connect()
            self._drop_table(table_name)

        except Exception as e:
            raise Exception(f"Failed to create table: {e}")

        else:
            self._schema.remove_table(table_name)

        finally:
            self._close()


    @typeguard.typechecked
    def add_column(
            self,
            table_name: str,
            column_name: str,
            column_type: str
        ) -> None:
        if not table_name in self._schema.tables():
            raise Exception("Table name not found on schema tables.")

        if column_name in self._schema.columns(table_name):
            raise Exception("Column name already exists on schema table.")

        try:
            self._connect()
            self._add_column(table_name, column_name, column_type)

        except Exception as e:
            raise Exception(f"Failed to add column to table: {e}")

        else:
            self._schema.add_column(table_name, column_name, column_type)

        finally:
            self._close()


    @typeguard.typechecked
    def drop_column(
            self,
            table_name: str,
            column_name: str
        ) -> None:
        if not table_name in self._schema.tables():
            raise Exception("Table name not found on schema tables.")

        if not column_name in self._schema.columns(table_name):
            raise Exception("Column name not found on schema table.")

        try:
            self._connect()
            self._drop_column(table_name, column_name)

        except Exception as e:
            raise Exception(f"Failed to drop column from table: {e}")

        else:
            self._schema.remove_column(table_name, column_name)

        finally:
            self._close()


    @typeguard.typechecked
    def insert(
            self,
            table_name: str,
            data: typing.Dict[str, typing.Any]
        ) -> int:
        if not table_name in self._schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self._connect()
            return self._insert(table_name, data)

        except Exception as e:
            raise Exception(f"Failed to insert record: {e}")

        finally:
            self._close()


    @typeguard.typechecked
    def extend(
            self,
            table_name: str,
            data: polars.DataFrame
        ) -> int:
        if not table_name in self._schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self._connect()
            return self._extend(table_name, data)

        except Exception as e:
            raise Exception(f"Failed to insert record: {e}")

        finally:
            self._close()


    @typeguard.typechecked
    def read(
            self,
            table_name: str,
            conditions: typing.Optional[typing.Dict[str, typing.Any]] = None
        ) -> polars.DataFrame:
        if not table_name in self._schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self._connect()
            return self._read(table_name, conditions)

        except Exception as e:
            raise Exception(f"Failed to read records: {e}")

        finally:
            self._close()


    @typeguard.typechecked
    def update(
            self,
            table_name: str,
            data: typing.Dict[str, typing.Any],
            conditions: typing.Dict[str, typing.Any]
        ) -> int:
        if not table_name in self._schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self._connect()
            return self._update(table_name, data, conditions)

        except Exception as e:
            raise Exception(f"Failed to update records: {e}")

        finally:
            self._close()


    @typeguard.typechecked
    def delete(
            self,
            table_name: str,
            conditions: typing.Dict[str, typing.Any]
        ) -> int:
        if not table_name in self._schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self._connect()
            return self._delete(table_name, conditions)

        except Exception as e:
            raise Exception(f"Failed to delete records: {e}")

        finally:
            self._close()


    @typeguard.typechecked
    def validate_schema(
            self,
            can_load_schema: bool = False,
            can_purge: bool = False
        ) -> None:
        try:
            self._connect()
            self._validate_schema(can_load_schema = can_load_schema, can_purge = can_purge)

        except Exception as e:
            raise Exception(f"Failed to validate database schema: {e}")

        finally:
            self._close()


    @abc.abstractmethod
    def _connect(self) -> None:
        pass


    @abc.abstractmethod
    def _close(self) -> None:
        pass


    @abc.abstractmethod
    def _create_table(
            self,
            table_name: str,
            columns: typing.Dict[str, str]
        ) -> None:
        pass


    @abc.abstractmethod
    def _drop_table(
            self,
            table_name: str
        ) -> None:
        pass


    @abc.abstractmethod
    def _add_column(
            self,
            table_name: str,
            column_name: str,
            column_type: str
        ) -> None:
        pass


    @abc.abstractmethod
    def _drop_column(
            self,
            table_name: str,
            column_name: str
        ) -> None:
        pass


    @abc.abstractmethod
    def _insert(
            self,
            table_name: str,
            data: typing.Dict[str, typing.Any]
        ) -> int:
        pass


    @abc.abstractmethod
    def _extend(
            self,
            table_name: str,
            data: polars.DataFrame
        ) -> int:
        pass


    @abc.abstractmethod
    def _read(
            self,
            table_name: str,
            conditions: typing.Optional[typing.Dict[str, typing.Any]] = None
        ) -> polars.DataFrame:
        pass


    @abc.abstractmethod
    def _update(
            self,
            table_name: str,
            data: typing.Dict[str, typing.Any],
            conditions: typing.Dict[str, typing.Any]
        ) -> int:
        pass


    @abc.abstractmethod
    def _delete(
            self,
            table_name: str,
            conditions: typing.Dict[str, typing.Any]
        ) -> int:
        pass


    @abc.abstractmethod
    def _validate_schema(
            self,
            can_load_schema: bool,
            can_purge: bool
        ) -> None:
        pass