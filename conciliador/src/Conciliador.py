import datetime
import pathlib
import polars
import sqlalchemy
import typeguard
import typing

from .database import Database
from .database.join import Join, JoinTypeEnum
from .loaders import ReportLoader, StatementLoader
from .utils import Currency


@typeguard.typechecked
class Conciliador():

    def __init__(
            self,
            database_uri: str,
            database_log_path: pathlib.Path,
            currency: str = "USD",
            thousands: str = ",",
            decimals: str = ".",
            has_dev_mode: bool = False
        ) -> None:
        self.__database: Database.Database = Database.Database(
            database_uri,
            database_log_path,
            has_dev_mode = has_dev_mode
        )
        self.__currency: Currency.Currency = Currency.Currency(
            currency,
            thousands = thousands,
            decimals = decimals
        )


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
        last_id = self.__database.extend("report", reports_df)[0]
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
                polars.col("name").alias("type"),
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
        concat_df = polars.concat(dataframes, how = "vertical")
        concat_df = concat_df.select(
            [
                polars.col("Data").str.strptime(polars.Date, "%d/%m/%Y").alias("date"),
                polars.col("Histórico").cast(polars.String).alias("name"),
                polars.col("Valor").str.replace_all(r"[,.]", "").cast(polars.Int64).alias("value"),
            ]
        )

        # Create statements dataframe
        statements_df = concat_df.select(
            [
                polars.col("date").alias("date"),
            ]
        ).unique()

        # Extend database with reports and recover ids
        last_id = self.__database.extend("statement", statements_df)[0]
        statements_df = statements_df.with_columns([
            (polars.Series(name = "id", values = [last_id - (statements_df.height - i - 1) for i in range(statements_df.height)]))
        ])

        # Link reports to statement entries with recovered id
        concat_df = concat_df.join(
            statements_df,
            on = [col for col in concat_df.columns if col in statements_df.columns],
            how = "left"
        )

        # Create statement entries dataframe
        statement_entries_df = concat_df.select(
            [
                polars.col("id").alias("statement_id"),
                polars.col("name").alias("name"),
                polars.col("value").alias("value"),
            ]
        )

        # Extend database with finishers
        self.__database.extend("statement_entry", statement_entries_df)


    def link(
            self,
            start_date: datetime.date,
            end_date: datetime.date
        ) -> None:
        # TODO
        # To link finisher and statement entry:
        current_date = start_date - datetime.timedelta(days = 1)
        while current_date < end_date:
            current_date += datetime.timedelta(days = 1)

            types: typing.List[str] = []
            print(self.__database.read(
                "report",
                # ["finisher.type"],
                # distinct = True,
                joins = [
                    Join.Join("report", "finisher", lambda x, y: x.id == y.report_id, JoinTypeEnum.JoinTypeEnum.INNER),
                    # Join.Join("statement_entry", lambda x, y: x.type == y.type, JoinTypeEnum.JoinTypeEnum.FULL_OUTER),
                    # Join.Join("statement", lambda x, y: x.statement_id == y.id, JoinTypeEnum.JoinTypeEnum.INNER),
                ],
                conditions = {
                    "report.start_time": lambda x: sqlalchemy.and_(
                        x >= datetime.datetime.min.replace(current_date.year, current_date.month, current_date.day),
                        x <= datetime.datetime.max.replace(current_date.year, current_date.month, current_date.day)
                    ),
                    # "statement.date": lambda x: x == current_date,
                }
            ))
            # day_reports_ids: typing.List[int] = self.__database.read(
            #     "report", 
            #     start_time = lambda x: sqlalchemy.and_(
            #         x >= datetime.datetime.min.replace(current_date.year, current_date.month, current_date.day),
            #         x <= datetime.datetime.max.replace(current_date.year, current_date.month, current_date.day)
            #     )
            # )["id"].to_list()
            # if not day_reports_ids:
            #     continue
            # day_finishers: polars.DataFrame = self.__database.read(
            #     "finisher",
            #     report_id = lambda x: x.in_(day_reports_ids)
            # )
            # if day_finishers.is_empty():
            #     continue
            # print(day_finishers.to_pandas().head(100))

            # day_statement_ids: typing.List[int] = self.__database.read(
            #     "statement",
            #     date = lambda x: x == current_date
            # )["id"].to_list()
            # if not day_statement_ids:
            #     continue
            # day_statement_id: int = day_statement_ids[0]
            # day_statement_entries: polars.DataFrame = self.__database.read(
            #     "statement_entry",
            #     statement_id = lambda x: x == day_statement_id
            # )
            # if day_statement_entries.is_empty():
            #     continue
            # print(day_statement_entries.to_pandas().head(100))

            # types = UniqueList.UniqueList()
            # types.extend(day_finishers["type"].to_list())
            # types.extend(day_statement_entries["type"].to_list())

            # for type in types:
            #     print(type)


if __name__ == "__main__":
    c = Conciliador()