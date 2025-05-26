import datetime
import pathlib
import polars
import typeguard

from . import Currency
from .database import Database
from .loaders import ReportLoader, StatementLoader


@typeguard.typechecked
class Conciliador():

    def __init__(
            self,
            database_uri: str,
            currency: str = "USD",
            thousands: str = ",",
            decimals: str = ".",
            has_dev_mode: bool = False
        ) -> None:
        self.__database: Database.Database = Database.Database(database_uri = database_uri, has_dev_mode = has_dev_mode)
        self.__currency: Currency.Currency = Currency.Currency(currency, thousands = thousands, decimals = decimals)


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
                polars.col("Turno").cast(polars.Int64).alias("shift"),
                polars.col("Funcionário").cast(polars.String).alias("employee"),
                polars.concat_str([polars.col("Data"), polars.lit(" "), polars.col("Início")]).str.strptime(polars.Datetime, "%d/%m/%Y %H:%M:%S").alias("start_time"),
                polars.concat_str([polars.col("Data"), polars.lit(" "), polars.col("Término")]).str.strptime(polars.Datetime, "%d/%m/%Y %H:%M:%S").alias("end_time"),
                polars.col("Finalizadora").cast(polars.String).alias("name"),
                polars.col("Total").str.replace_all(r"[,.]", "").cast(polars.Int64).alias("value"),
            ]
        )

        # Create reports dataframe
        reports_df = concat_df.select(
            [
                polars.col("shift").alias("shift"),
                polars.col("employee").alias("employee"),
                polars.col("start_time").alias("start_time"),
                polars.col("end_time").alias("end_time"),
            ]
        ).unique()

        # Extend database with reports and recover ids
        _, last_id = self.__database.extend("report", reports_df)
        reports_df = reports_df.with_columns([
            (polars.Series(name = "id", values = [last_id - (reports_df.height - i - 1) for i in range(reports_df.height)]))
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
                polars.col("Data").str.strptime(polars.Date, "%d/%m/%Y").alias("date"),
                polars.col("Histórico").cast(polars.String).alias("name"),
                polars.col("Valor").str.replace_all(r"[,.]", "").cast(polars.Int64).alias("value")
            ]
        )

        # Save dataframes into database
        self.__database.extend("statement", statements_df)


    def compile(
            self,
            start_date: datetime.date,
            end_date: datetime.date,
            can_overwrite_compiled: bool = False
        ) -> None:
        current_date = start_date

        while current_date <= end_date:
            self.__database.read("")

            current_date += datetime.timedelta(days = 1)


    def check(
            self
            #...
        ) -> None:
        ...


if __name__ == "__main__":
    c = Conciliador()