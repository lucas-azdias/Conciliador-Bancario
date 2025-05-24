import pathlib
import polars
import typeguard

from . import Currency
from .database import Database
from .loaders import ReportLoader, StatementLoader


class Conciliador():

    @typeguard.typechecked
    def __init__(
            self,
            database_uri: str,
            currency: str = "USD",
            thousands: str = ",",
            decimals: str = "."
        ) -> None:
        self.__database: Database.Database = Database.Database(database_uri = database_uri)
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

        # Concatenate dataframes and format them
        concat_df = polars.concat(dataframes, how = "vertical")
        concat_df = concat_df.select(
            [
                polars.col("Data").str.strptime(polars.Date, "%d/%m/%Y").dt.day().cast(polars.Int64).alias("day"),
                polars.col("Data").str.strptime(polars.Date, "%d/%m/%Y").dt.month().cast(polars.Int64).alias("month"),
                polars.col("Data").str.strptime(polars.Date, "%d/%m/%Y").dt.year().cast(polars.Int64).alias("year"),
                polars.col("Turno").cast(polars.Int64).alias("shift"),
                polars.col("Funcionário").cast(polars.String).alias("employee"),
                polars.col("Início").str.strptime(polars.Time, "%H:%M:%S").dt.hour().cast(polars.Int64).alias("start_hour"),
                polars.col("Início").str.strptime(polars.Time, "%H:%M:%S").dt.minute().cast(polars.Int64).alias("start_min"),
                polars.col("Início").str.strptime(polars.Time, "%H:%M:%S").dt.second().cast(polars.Int64).alias("start_sec"),
                polars.col("Término").str.strptime(polars.Time, "%H:%M:%S").dt.hour().cast(polars.Int64).alias("end_hour"),
                polars.col("Término").str.strptime(polars.Time, "%H:%M:%S").dt.minute().cast(polars.Int64).alias("end_min"),
                polars.col("Término").str.strptime(polars.Time, "%H:%M:%S").dt.second().cast(polars.Int64).alias("end_sec"),
                polars.col("Finalizadora").cast(polars.String).alias("name"),
                polars.col("Total").str.replace_all(r"[,.]", "").cast(polars.Int64).alias("value")
            ]
        )

        # Create reports dataframe
        reports_df = concat_df.select(
            [
                polars.col("day").alias("day"),
                polars.col("month").alias("month"),
                polars.col("year").alias("year"),
                polars.col("shift").alias("shift"),
                polars.col("employee").alias("employee"),
                polars.col("start_hour").alias("start_hour"),
                polars.col("start_min").alias("start_min"),
                polars.col("start_sec").alias("start_sec"),
                polars.col("end_hour").alias("end_hour"),
                polars.col("end_min").alias("end_min"),
                polars.col("end_sec").alias("end_sec"),
            ]
        ).unique()

        # Extend database with reports and recover ids
        report_last_row = self.__database.extend("report", reports_df)
        reports_df = reports_df.with_columns([
            (polars.Series(name = "id", values = [report_last_row - (reports_df.height - i - 1) for i in range(reports_df.height)]))
        ])

        # Link reports to finishers with recovered id
        concat_df = concat_df.join(
            reports_df,
            on = [col for col in concat_df.columns if col in reports_df.columns],
            how = "left"
        )

        # Create finishers dataframe
        finishers_df = concat_df.select(
            [
                polars.col("id").alias("report_id"),
                polars.col("name").alias("name"),
                polars.col("value").alias("value"),
            ]
        )

        # Extend database with finishers
        self.__database.extend("finisher", finishers_df)


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

        # Concatenate dataframes and create statements dataframe
        statements_df = polars.concat(dataframes, how = "vertical")
        statements_df = statements_df.select(
            [
                polars.col("Data").str.strptime(polars.Date, "%d/%m/%Y").dt.day().cast(polars.Int64).alias("day"),
                polars.col("Data").str.strptime(polars.Date, "%d/%m/%Y").dt.month().cast(polars.Int64).alias("month"),
                polars.col("Data").str.strptime(polars.Date, "%d/%m/%Y").dt.year().cast(polars.Int64).alias("year"),
                polars.col("Histórico").cast(polars.String).alias("name"),
                polars.col("Valor").str.replace_all(r"[,.]", "").cast(polars.Int64).alias("value")
            ]
        )

        # Save dataframes into database
        self.__database.extend("statement", statements_df)
        print(self.__database.read("statement"))


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