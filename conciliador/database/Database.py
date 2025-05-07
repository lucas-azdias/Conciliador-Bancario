import pathlib
import polars
import sqlite3
import typing


class Database():

    def __init__(
            self,
            schema: typing.Dict[str, typing.Iterable[str]],
            database_path: typing.Optional[pathlib.Path] = None
        ) -> None:
        self.__schema: typing.Dict[str, typing.Iterable[str]] = schema
        self.__database_path: str = database_path.absolute() if database_path else ":memory:"
        self.__conn: sqlite3.Connection = None
        self.__cursor: sqlite3.Cursor = None

        # Create database storage file if not created 
        self.__connect()
        self.__close()


    def create_table(
            self,
            table_name: str,
            columns: typing.Dict[str, str]
        ) -> None:
        if table_name in self.__schema:
            raise Exception("Table already exists in schema.")

        try:
            self.__connect()

            columns_def = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
            query = f"CREATE TABLE {table_name} ({columns_def})"

            self.__cursor.execute(query)
            self.__conn.commit()

        except sqlite3.Error as e:
            raise Exception(f"Failed to create table: {e}")

        finally:
            self.__schema[table_name] = columns
            self.__close()


    def insert(
            self,
            table_name: str,
            data: typing.Dict[str, typing.Any]
        ) -> int:
        try:
            self.__connect()

            columns = ', '.join(data.keys())
            placeholders = ', '.join(['?' for _ in data])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            self.__cursor.execute(query, list(data.values()))
            self.__conn.commit()

            return self.__cursor.lastrowid

        except sqlite3.Error as e:
            raise Exception(f"Failed to insert record: {e}")

        finally:
            self.__close()


    def read(
            self,
            table_name: str,
            conditions: typing.Optional[typing.Dict[str, typing.Any]] = None
        ) -> polars.DataFrame:
        try:
            self.__connect()

            query = f"SELECT * FROM {table_name}"
            params = []

            if conditions:
                where_clause = ' AND '.join([f"{k} = ?" for k in conditions.keys()])
                query += f" WHERE {where_clause}"
                params = list(conditions.values())

            fetched = polars.read_database(
                query = query,
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


    def update(
            self,
            table_name: str,
            data: typing.Dict[str, typing.Any],
            conditions: typing.Dict[str, typing.Any]
        ) -> int:
        try:
            self.__connect()

            set_clause = ', '.join([f"{k} = ?" for k in data.keys()])
            where_clause = ' AND '.join([f"{k} = ?" for k in conditions.keys()])
            query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
            params = list(data.values()) + list(conditions.values())
            
            self.__cursor.execute(query, params)
            self.__conn.commit()

            return self.__cursor.rowcount

        except sqlite3.Error as e:
            raise Exception(f"Failed to update records: {e}")

        finally:
            self.__close()


    def delete(
            self,
            table_name: str,
            conditions: typing.Dict[str, typing.Any]
        ) -> int:
        try:
            self.__connect()

            where_clause = ' AND '.join([f"{k} = ?" for k in conditions.keys()])
            query = f"DELETE FROM {table_name} WHERE {where_clause}"

            self.__cursor.execute(query, list(conditions.values()))
            self.__conn.commit()

            return self.__cursor.rowcount

        except sqlite3.Error as e:
            raise Exception(f"Failed to delete records: {e}")

        finally:
            self.__close()


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


    def query_commit(
            self,
            query: str,
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
            raise Exception(f"Failed to query and fetch data into dataframe: {e}")

        finally:
            self.__close()


    def __connect(self):
        try:
            self.__conn = sqlite3.connect(self.__database_path)
            self.__cursor = self.__conn.cursor()
        except sqlite3.Error as e:
            raise Exception(f"Failed to connect to database: {e}")


    def __close(self):
        if self.__cursor:
            self.__cursor.close()
        if self.__conn:
            self.__conn.close()


    def __validate_schema(self) -> None:
        try:
            self.__connect()

            # Get all tables
            self.__cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            existing_tables = {row[0] for row in self.__cursor.fetchall()}

            for table, expected_columns in self.__schema.items():
                if table not in existing_tables:
                    raise ValueError(f"Missing expected table: \"{table}\"")

                # Get actual column info
                self.__cursor.execute(f"PRAGMA table_info({table});")
                actual_columns = {row[1]: row[2].upper() for row in self.__cursor.fetchall()}

                for col_name, expected_type in expected_columns.items():
                    if col_name not in actual_columns:
                        raise ValueError(f"Missing column \"{col_name}\" in table \"{table}\"")

                    actual_type = actual_columns[col_name]
                    if actual_type != expected_type:
                        raise ValueError(
                            f"Type mismatch in table \"{table}\", column \"{col_name}\": "
                            f"expected {expected_type}, got {actual_type}"
                        )

        except sqlite3.Error as e:
            raise Exception(f"Failed to validate database schema: {e}")

        finally:
            self.__close()


if __name__ == "__main__":
    db = Database(
        {
            "SQLITE": ["version", "message"]
        }
    )
    print(db.query_fetch("SELECT sqlite_version();"))