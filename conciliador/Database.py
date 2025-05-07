import pathlib
import polars
import sqlite3
import typing


class Database():

    def __init__(
            self,
            database_path: typing.Optional[pathlib.Path] = None
        ) -> None:
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
        try:
            self.__connect()

            columns_def = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
            query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_def})"

            self.__cursor.execute(query)
            self.__conn.commit()

        except sqlite3.Error as e:
            raise Exception(f"Failed to create table: {e}")

        finally:
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


if __name__ == "__main__":
    db = Database()
    print(db.query_fetch("SELECT sqlite_version();"))