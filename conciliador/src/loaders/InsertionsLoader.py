import json
import pathlib
import polars
import typeguard
import typing

from . import Loader


@typeguard.typechecked
class InsertionsLoader(Loader.Loader[typing.Dict[str, polars.DataFrame]]):

    def process_file(
            self,
            path: pathlib.Path,
            encoding: typing.Optional[str] = None
        ) -> typing.Dict[str, polars.DataFrame]:
        with open(path, mode = "r", encoding = encoding or Loader.Loader.detect_encoding(path)) as file:
            data: typing.Dict[str, typing.List[typing.Dict[str, str]]] = json.load(file)

        dataframes: typing.Dict[str, polars.DataFrame] = dict()
        for table_name, insertions in data.items():
            dataframes[table_name] = polars.from_dicts(insertions)

        return dataframes