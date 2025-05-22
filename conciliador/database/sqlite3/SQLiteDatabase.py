import pathlib
import polars
import sqlite3
import typeguard
import typing

from .. import Database
from . import SQLiteSchema


class SQLiteDatabase(Database.Database):

    @typeguard.typechecked
    def __init__(
            self,
            schema: SQLiteSchema.SQLiteSchema,
            database_path: typing.Optional[pathlib.Path] = None,
            can_load_schema: bool = False,
            can_purge: bool = False
        ) -> None:
        self._conn: sqlite3.Connection = None
        self._cursor: sqlite3.Cursor = None

        super().__init__(
            schema = schema,
            database_path = database_path,
            can_load_schema = can_load_schema,
            can_purge = can_purge
        )


    @typeguard.typechecked
    def _connect(self) -> None:
        try:
            self._conn = sqlite3.connect(f"sqlite:///{self._database_path.absolute() if self._database_path else ':memory:'}")
            self._cursor = self._conn.cursor()
        except Exception as e:
            raise Exception(f"Failed to connect to database: {e}")


    @typeguard.typechecked
    def _close(self) -> None:
        if self._cursor:
            self._cursor.close()
        if self._conn:
            self._conn.close()


    @typeguard.typechecked
    def _create_table(
            self,
            table_name: str,
            columns: typing.Dict[str, str]
        ) -> None:
        columns_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        query = f"CREATE TABLE {table_name} ({columns_def}) STRICT;"

        self._cursor.execute(query)
        self._conn.commit()


    @typeguard.typechecked
    def _drop_table(
            self,
            table_name: str
        ) -> None:
        query = f"DROP TABLE {table_name}"

        self._cursor.execute(query)
        self._conn.commit()


    @typeguard.typechecked
    def _add_column(
            self,
            table_name: str,
            column_name: str,
            column_type: str
        ) -> None:
        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"

        self._cursor.execute(query)
        self._conn.commit()


    @typeguard.typechecked
    def _drop_column(
            self,
            table_name: str,
            column_name: str
        ) -> None:
        query = f"ALTER TABLE {table_name} DROP COLUMN {column_name};"

        self._cursor.execute(query)
        self._conn.commit()


    @typeguard.typechecked
    def _insert(
            self,
            table_name: str,
            data: typing.Dict[str, typing.Any]
        ) -> int:
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"

        self._cursor.execute(query, list(data.values()))
        self._conn.commit()

        return self._cursor.lastrowid


    @typeguard.typechecked
    def _extend(
            self,
            table_name: str,
            data: polars.DataFrame
        ) -> int:
        # TODO No current support to write_database with an SQLite3 connection (maybe later)
        # data.write_database(table_name, self._conn, if_table_exists = "append")

        pd_data = data.to_pandas()
        pd_data.to_sql(table_name, self._conn, if_exists = "append", index = False)

        # Get last id (pandas.to_sql doesn't work with self._cursor.lastrowid)
        self._cursor.execute(f"SELECT MAX({self._schema.id_column}) FROM {table_name}")
        last_id = self._cursor.fetchone()[0]

        return last_id


    @typeguard.typechecked
    def _read(
            self,
            table_name: str,
            conditions: typing.Optional[typing.Dict[str, typing.Any]] = None
        ) -> polars.DataFrame:
        query = f"SELECT * FROM {table_name}"
        params = []

        if conditions:
            where_clause = " AND ".join([f"{k} = ?" for k in conditions.keys()])
            query += f" WHERE {where_clause}"
            params = list(conditions.values())

        fetched = polars.read_database(
            query = query + ";",
            connection = self._conn,
            execute_options = {
                "parameters": params
            } if params else None
        )
        return fetched


    @typeguard.typechecked
    def _update(
            self,
            table_name: str,
            data: typing.Dict[str, typing.Any],
            conditions: typing.Dict[str, typing.Any]
        ) -> int:
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
        where_clause = " AND ".join([f"{k} = ?" for k in conditions.keys()])
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
        params = list(data.values()) + list(conditions.values())
        
        self._cursor.execute(query, params)
        self._conn.commit()

        return self._cursor.rowcount


    @typeguard.typechecked
    def _delete(
            self,
            table_name: str,
            conditions: typing.Dict[str, typing.Any]
        ) -> int:
        where_clause = " AND ".join([f"{k} = ?" for k in conditions.keys()])
        query = f"DELETE FROM {table_name} WHERE {where_clause};"

        self._cursor.execute(query, list(conditions.values()))
        self._conn.commit()

        return self._cursor.rowcount


    @typeguard.typechecked
    def _validate_schema(
            self,
            can_load_schema: bool,
            can_purge: bool
        ) -> None:
        try:
            self._connect()

            # Get all tables
            self._cursor.execute("SELECT name FROM sqlite_master WHERE type=\"table\";")
            existing_tables = {row[0] for row in self._cursor.fetchall()}
            schema_tables = self._schema.tables()

            # Verify if all schema tables are in database
            for table_name in schema_tables:
                if table_name not in existing_tables:
                    if not can_load_schema:
                        raise ValueError(f"Missing expected table on database: \"{table_name}\".")
                    columns = self._schema.columns(table_name)
                    columns_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
                    query = f"CREATE TABLE {table_name} ({columns_def}) STRICT;"
                    self._cursor.execute(query)
                    self._conn.commit()
                    continue

                # Get actual column info
                self._cursor.execute(f"PRAGMA table_info({table_name});")
                existing_columns = {row[1]: row[2].upper() for row in self._cursor.fetchall()}
                schema_columns = self._schema.columns(table_name)

                # Verify if all schema table columns are in database
                for column_name, column_type in schema_columns.items():
                    if column_name not in existing_columns:
                        if not can_load_schema:
                            raise ValueError(f"Missing column \"{column_name}\" in table \"{table_name}\" on database.")
                        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"
                        self._cursor.execute(query)
                        self._conn.commit()
                        continue

                    existing_type = existing_columns[column_name]
                    if column_type != existing_type or (
                        column_name == self._schema.id_column and column_type.split(" ")[0] == existing_type
                    ):
                        raise ValueError(
                            f"Type mismatch in table \"{table_name}\", column \"{column_name}\" on database: "
                            f"expected \"{column_type}\", got \"{existing_type}\"."
                        )

                # Verify if all database table columns are in schema
                for column_name in existing_columns:
                    if not column_name in self._schema.columns(table_name):
                        if not can_purge:
                            raise ValueError(f"Database column \"{column_name}\" in table \"{table_name}\" not found on schema.")
                        query = f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
                        self._cursor.execute(query)
                        self._conn.commit()

            # Verify if all database tables are in schema
            for table_name in existing_tables:
                if not table_name in self._schema.tables() and not table_name in self._schema.default_tables:
                    if not can_purge:
                        raise ValueError(f"Database table \"{table_name}\" not found on schema.")
                    query = f"DROP TABLE {table_name}"
                    self._cursor.execute(query)
                    self._conn.commit()

        except Exception as e:
            raise Exception(f"Failed to validate database schema: {e}")

        finally:
            self._close()