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

        # Creates database storage file if not created 
        conn = sqlite3.connect(self.__database_path)
        conn.close()


        # Connect to database
    def query_fetch(self, query: str, fetch_size: typing.Optional[int] = None) -> typing.Tuple[typing.Any]:
        conn = sqlite3.connect(self.__database_path)

        # Get cursor
        cursor = conn.cursor()

        # Execute
        cursor.execute(query)

        # Fetch result by size given
        if not fetch_size:
            fetched = cursor.fetchall()
        elif fetch_size == 1:
            fetched = [cursor.fetchone()]
        else:
            fetched = cursor.fetchmany(fetch_size)

        # Close connection
        conn.close()

        return tuple(fetched)


    def query_dataframe(self, query: str) -> polars.DataFrame:
        # Connect to database
        conn = sqlite3.connect(self.__database_path)

        # Read query into dataframe
        fetched = polars.read_database(query = query, connection = conn)

        # Close connection
        conn.close()

        return fetched


    def query_commit(self, query: str, *parameters: typing.Iterable) -> None:
        # Connect to database
        conn = sqlite3.connect(self.__database_path)

        # Get cursor
        cursor = conn.cursor()

        # Execute with parameters if any
        if len(parameters) <= 1:
            cursor.execute(query, parameters)
        else:
            cursor.executemany(query, parameters)

        # Commit operations
        conn.commit()

        # Close connection
        conn.close()


if __name__ == "__main__":
    db = Database()
    print(db.query_fetch("SELECT sqlite_version();"))