import pathlib
import polars
import sqlite3
import typeguard
import typing

from . import Schema


class Database():

    @typeguard.typechecked
    def __init__(
            self,
            schema: Schema.Schema,
            database_path: typing.Optional[pathlib.Path] = None,
            can_load_schema: bool = False, # Enables add tables/columns from schema to the database
            can_purge: bool = False # Enables purge tables/columns from database based on schema
        ) -> None:
        self.__schema: Schema.Schema = schema
        self.__database_path: str = database_path.absolute() if database_path else ":memory:"
        self.__conn: sqlite3.Connection = None
        self.__cursor: sqlite3.Cursor = None

        # Create database storage file if not created 
        self.__connect()
        self.__close()

        # Validates schema
        self.__validate_schema(can_load_schema = can_load_schema, can_purge = can_purge)


    @typeguard.typechecked
    def create_table(
            self,
            table_name: str,
            columns: typing.Dict[str, str]
        ) -> None:
        if table_name in self.__schema.tables():
            raise Exception("Table name already exists on schema tables.")

        try:
            self.__connect()

            columns_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
            query = f"CREATE TABLE {table_name} ({columns_def}) STRICT;"

            self.__cursor.execute(query)
            self.__conn.commit()

        except sqlite3.Error as e:
            raise Exception(f"Failed to create table: {e}")

        else:
            self.__schema.add_table(table_name, columns)

        finally:
            self.__close()


    @typeguard.typechecked
    def drop_table(
            self,
            table_name: str
        ) -> None:
        if not table_name in self.__schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self.__connect()

            query = f"DROP TABLE {table_name}"

            self.__cursor.execute(query)
            self.__conn.commit()

        except sqlite3.Error as e:
            raise Exception(f"Failed to create table: {e}")

        else:
            self.__schema.remove_table(table_name)

        finally:
            self.__close()


    @typeguard.typechecked
    def add_column(
            self,
            table_name: str,
            column_name: str,
            column_type: str
        ) -> None:
        if not table_name in self.__schema.tables():
            raise Exception("Table name not found on schema tables.")

        if column_name in self.__schema.columns(table_name):
            raise Exception("Column name already exists on schema table.")

        try:
            self.__connect()

            query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"

            self.__cursor.execute(query)
            self.__conn.commit()

        except sqlite3.Error as e:
            raise Exception(f"Failed to add column to table: {e}")

        else:
            self.__schema.add_column(table_name, column_name, column_type)

        finally:
            self.__close()


    @typeguard.typechecked
    def drop_column(
            self,
            table_name: str,
            column_name: str
        ) -> None:
        if not table_name in self.__schema.tables():
            raise Exception("Table name not found on schema tables.")

        if not column_name in self.__schema.columns(table_name):
            raise Exception("Column name not found on schema table.")

        try:
            self.__connect()

            query = f"ALTER TABLE {table_name} DROP COLUMN {column_name};"

            self.__cursor.execute(query)
            self.__conn.commit()

        except sqlite3.Error as e:
            raise Exception(f"Failed to drop column from table: {e}")

        else:
            self.__schema.remove_column(table_name, column_name)

        finally:
            self.__close()


    @typeguard.typechecked
    def insert(
            self,
            table_name: str,
            data: typing.Dict[str, typing.Any]
        ) -> int:
        if not table_name in self.__schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self.__connect()

            columns = ", ".join(data.keys())
            placeholders = ", ".join(["?" for _ in data])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"

            self.__cursor.execute(query, list(data.values()))
            self.__conn.commit()

            return self.__cursor.lastrowid

        except sqlite3.Error as e:
            raise Exception(f"Failed to insert record: {e}")

        finally:
            self.__close()


    @typeguard.typechecked
    def extend(
            self,
            table_name: str,
            data: polars.DataFrame
        ) -> int:
        if not table_name in self.__schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self.__connect()

            # TODO No current support to write_database with an SQLite3 connection (maybe later)
            # data.write_database(table_name, self.__conn, if_table_exists = "append")

            pd_data = data.to_pandas()
            pd_data.to_sql(table_name, self.__conn, if_exists = "append", index = False)

            # Get last id (pandas.to_sql doesn't work with self.__cursor.lastrowid)
            self.__cursor.execute(f"SELECT MAX(id) FROM {table_name}")
            last_id = self.__cursor.fetchone()[0]

            return last_id

        except sqlite3.Error as e:
            raise Exception(f"Failed to insert record: {e}")

        finally:
            self.__close()

    @typeguard.typechecked
    def read(
            self,
            table_name: str,
            conditions: typing.Optional[typing.Dict[str, typing.Any]] = None
        ) -> polars.DataFrame:
        if not table_name in self.__schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self.__connect()

            query = f"SELECT * FROM {table_name}"
            params = []

            if conditions:
                where_clause = " AND ".join([f"{k} = ?" for k in conditions.keys()])
                query += f" WHERE {where_clause}"
                params = list(conditions.values())

            fetched = polars.read_database(
                query = query + ";",
                connection = self.__conn,
                execute_options = {
                    "parameters": params
                } if params else None
            )
            return fetched

        except sqlite3.Error as e:
            raise Exception(f"Failed to read records: {e}")

        finally:
            self.__close()


    @typeguard.typechecked
    def update(
            self,
            table_name: str,
            data: typing.Dict[str, typing.Any],
            conditions: typing.Dict[str, typing.Any]
        ) -> int:
        if not table_name in self.__schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self.__connect()

            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            where_clause = " AND ".join([f"{k} = ?" for k in conditions.keys()])
            query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
            params = list(data.values()) + list(conditions.values())
            
            self.__cursor.execute(query, params)
            self.__conn.commit()

            return self.__cursor.rowcount

        except sqlite3.Error as e:
            raise Exception(f"Failed to update records: {e}")

        finally:
            self.__close()


    @typeguard.typechecked
    def delete(
            self,
            table_name: str,
            conditions: typing.Dict[str, typing.Any]
        ) -> int:
        if not table_name in self.__schema.tables():
            raise Exception("Table name not found on schema tables.")

        try:
            self.__connect()

            where_clause = " AND ".join([f"{k} = ?" for k in conditions.keys()])
            query = f"DELETE FROM {table_name} WHERE {where_clause};"

            self.__cursor.execute(query, list(conditions.values()))
            self.__conn.commit()

            return self.__cursor.rowcount

        except sqlite3.Error as e:
            raise Exception(f"Failed to delete records: {e}")

        finally:
            self.__close()


    @typeguard.typechecked
    def query_fetch(
            self,
            query: str,
            fetch_size: typing.Optional[int] = None
        ) -> typing.Tuple[typing.Any]:
        try:
            self.__connect()

            self.__cursor.execute(query)

            # Fetch result by size given
            if not fetch_size:
                fetched = self.__cursor.fetchall()
            elif fetch_size == 1:
                fetched = [self.__cursor.fetchone()]
            else:
                fetched = self.__cursor.fetchmany(fetch_size)

        except sqlite3.Error as e:
            raise Exception(f"Failed to query and fetch data: {e}")

        finally:
            self.__close()

        return tuple(fetched)


    @typeguard.typechecked
    def query_dataframe(
            self,
            query: str
        ) -> polars.DataFrame:
        try:
            self.__connect()

            # Read query into dataframe
            fetched = polars.read_database(query = query, connection = self.__conn)

        except sqlite3.Error as e:
            raise Exception(f"Failed to query and fetch data into dataframe: {e}")

        finally:
            self.__close()

        return fetched


    @typeguard.typechecked
    def query_commit(
            self,
            query: str,
            can_load_schema: bool = False,
            can_purge: bool = False,
            *parameters: typing.Iterable
        ) -> None:
        try:
            self.__connect()

            # Execute with parameters if any
            if len(parameters) <= 1:
                self.__cursor.execute(query, parameters)
            else:
                self.__cursor.executemany(query, parameters)

            # Commit operations
            self.__conn.commit()

        except sqlite3.Error as e:
            raise Exception(f"Failed to query and commit: {e}")

        else:
            # Validate schema for changes
            self.__validate_schema(can_load_schema = can_load_schema, can_purge = can_purge)

        finally:
            self.__close()


    @typeguard.typechecked
    def __connect(self) -> None:
        try:
            self.__conn = sqlite3.connect(self.__database_path)
            self.__cursor = self.__conn.cursor()
        except sqlite3.Error as e:
            raise Exception(f"Failed to connect to database: {e}")


    @typeguard.typechecked
    def __close(self) -> None:
        if self.__cursor:
            self.__cursor.close()
        if self.__conn:
            self.__conn.close()


    @typeguard.typechecked
    def __validate_schema(
            self,
            can_load_schema,
            can_purge
        ) -> None:
        try:
            self.__connect()

            # Get all tables
            self.__cursor.execute("SELECT name FROM sqlite_master WHERE type=\"table\";")
            existing_tables = {row[0] for row in self.__cursor.fetchall()}
            schema_tables = self.__schema.tables()

            # Verify if all schema tables are in database
            for table_name in schema_tables:
                if table_name not in existing_tables:
                    if not can_load_schema:
                        raise ValueError(f"Missing expected table on database: \"{table_name}\".")
                    columns = self.__schema.columns(table_name)
                    columns_def = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
                    query = f"CREATE TABLE {table_name} ({columns_def}) STRICT;"
                    self.__cursor.execute(query)
                    self.__conn.commit()
                    continue

                # Get actual column info
                self.__cursor.execute(f"PRAGMA table_info({table_name});")
                existing_columns = {row[1]: row[2].upper() for row in self.__cursor.fetchall()}
                schema_columns = self.__schema.columns(table_name)

                # Verify if all schema table columns are in database
                for column_name, column_type in schema_columns.items():
                    if column_name not in existing_columns:
                        if not can_load_schema:
                            raise ValueError(f"Missing column \"{column_name}\" in table \"{table_name}\" on database.")
                        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type};"
                        self.__cursor.execute(query)
                        self.__conn.commit()
                        continue

                    existing_type = existing_columns[column_name]
                    if column_type.split(" ")[0] != existing_type:
                        raise ValueError(
                            f"Type mismatch in table \"{table_name}\", column \"{column_name}\" on database: "
                            f"expected \"{column_type}\", got \"{existing_type}\"."
                        )

                # Verify if all database table columns are in schema
                for column_name in existing_columns:
                    if not column_name in self.__schema.columns(table_name):
                        if not can_purge:
                            raise ValueError(f"Database column \"{column_name}\" in table \"{table_name}\" not found on schema.")
                        query = f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
                        self.__cursor.execute(query)
                        self.__conn.commit()

            # Verify if all database tables are in schema
            for table_name in existing_tables:
                if not table_name in self.__schema.tables() and not table_name in Schema.DEFAULT_TABLES:
                    if not can_purge:
                        raise ValueError(f"Database table \"{table_name}\" not found on schema.")
                    query = f"DROP TABLE {table_name}"
                    self.__cursor.execute(query)
                    self.__conn.commit()

        except sqlite3.Error as e:
            raise Exception(f"Failed to validate database schema: {e}")

        finally:
            self.__close()