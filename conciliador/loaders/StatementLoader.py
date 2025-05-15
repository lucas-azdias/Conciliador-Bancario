import pathlib
import polars
import typeguard
import typing

from . import Loader


STATEMENT_COLUMNS = ("Data", "Histórico", "Valor")


class StatementLoader(Loader.Loader):

    @typeguard.typechecked
    def __init__(
            self,
            path_filter: str,
            input: pathlib.Path,
            archive: pathlib.Path,
            encoding: typing.Optional[str] = None
        ) -> None:
        super().__init__(
            path_filter = path_filter,
            input = input,
            archive = archive,
            encoding = encoding
        )


    @typeguard.typechecked
    def process_file(
            self,
            path: pathlib.Path,
            encoding: str
        ) -> polars.DataFrame:
        df = polars.read_csv(path, separator = ";", encoding = encoding)
        df.columns = STATEMENT_COLUMNS
        return df