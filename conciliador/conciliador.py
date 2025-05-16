import pathlib
import polars
import typeguard

from . import Currency
from .database import Database, Schema
from .loaders import ReportLoader, StatementLoader


class Conciliador():

    @typeguard.typechecked
    def __init__(
            self,
            db_schema_path: pathlib.Path = pathlib.Path("db/db_schema.json"),
            database_path: pathlib.Path = pathlib.Path("db/database.db"),
            currency: str = "USD",
            thousands: str = ",",
            decimals: str = "."
        ) -> None:
        self.__database: Database.Database = Database.Database(Schema.Schema("id", path = db_schema_path), database_path = database_path, can_load_schema = True)
        self.__currency: Currency.Currency = Currency.Currency(currency, thousands = thousands, decimals = decimals)


    @typeguard.typechecked
    def load_reports(
            self,
            input: pathlib.Path,
            archive: pathlib.Path,
            can_archive: bool = False,
            can_overwrite_archive: bool = False
        ) -> None:
        loader = ReportLoader.ReportLoader()

        # Load files and archive them
        paths = loader.extract_paths(input, folder_filter = "*.csv")
        dataframes = loader.process_files(paths)
        if can_archive:
            loader.archive_files(paths, archive, can_overwrite_archive = can_overwrite_archive)

        # Check if no data was found
        if not dataframes:
            raise Exception("No data was found.")

        # Save dataframes into database
        concat_df = polars.concat(dataframes, how = "vertical")
        print(self.__database.extend("Report", concat_df))


    @typeguard.typechecked
    def load_statements(
            self,
            input: pathlib.Path,
            archive: pathlib.Path,
            can_archive: bool = False,
            can_overwrite_archive: bool = False
        ) -> None:
        loader = StatementLoader.StatementLoader()

        # Load files and archive them
        paths = loader.extract_paths(input, folder_filter = "*.csv")
        dataframes = loader.process_files(paths)
        if can_archive:
            loader.archive_files(paths, archive, can_overwrite_archive = can_overwrite_archive)

        # Check if no data was found
        if not dataframes:
            raise Exception("No data was found.")

        # Save dataframes into database
        concat_df = polars.concat(dataframes, how = "vertical")
        print(self.__database.extend("Statements", concat_df))


    @typeguard.typechecked
    def compile(
            self
            #...
        ) -> None:
        ...


    @typeguard.typechecked
    def check(
            self
            #...
        ) -> None:
        ...


if __name__ == "__main__":
    c = Conciliador()