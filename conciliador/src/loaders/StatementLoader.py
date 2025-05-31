import pathlib
import polars
import typeguard
import typing

from . import Loader


STATEMENT_COLUMNS = ("Data", "Histórico", "Valor")


@typeguard.typechecked
class StatementLoader(Loader.Loader[polars.DataFrame]):

    def process_file(
            self,
            path: pathlib.Path,
            encoding: typing.Optional[str] = None
        ) -> polars.DataFrame:
        df = polars.read_csv(path, separator = ";", encoding = encoding or Loader.Loader.detect_encoding(path))
        df.columns = STATEMENT_COLUMNS
        return df