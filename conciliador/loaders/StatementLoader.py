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
            archive: typing.Optional[pathlib.Path] = None,
        ) -> None:
        super().__init__(
            path_filter = path_filter,
            input = input,
            archive = archive,
        )


    @typeguard.typechecked
    def process_file(
            self,
            path: pathlib.Path,
            encoding: typing.Optional[str] = None
        ) -> polars.DataFrame:
        df = polars.read_csv(path, separator = ";", encoding = encoding or self.detect_encoding(path))
        df.columns = STATEMENT_COLUMNS
        return df