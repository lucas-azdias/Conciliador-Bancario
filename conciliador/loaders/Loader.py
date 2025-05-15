import abc
import chardet
import pathlib
import polars
import typeguard
import typing


class Loader(abc.ABC):

    @typeguard.typechecked
    def __init__(
            self,
            path_filter: str,
            input: pathlib.Path,
            archive: typing.Optional[pathlib.Path] = None,
        ) -> None:
        self.__path_filter: str = path_filter
        self.__input: pathlib.Path = input
        self.__archive: pathlib.Path = archive


    @typeguard.typechecked
    def process(
            self,
            encoding: typing.Optional[str] = None,
            can_archive_files = False
        ) -> typing.Tuple[polars.DataFrame]:
        paths: typing.List[pathlib.Path] = list()
        dataframes: typing.List[polars.DataFrame] = list()

        if self.__input.is_dir():
            paths.extend(self.__input.rglob(self.__path_filter))
        elif self.__input.is_file():
            paths.append(self.__input)
        else:
            raise Exception("Non file or folder path found.")

        for path in paths:
            try:
                dataframe = self.process_file(path, encoding or self.detect_encoding(path))
            except Exception as e:
                Exception(f"Error processing file \"{path}\": {e}")
            else:
                paths.append(path)
                dataframes.append(dataframe)

        # Archive files
        if can_archive_files:
            if not self.__archive:
                raise Exception("No archive folder was given.")

            self.__archive.mkdir(parents = True, exist_ok = True)
            for path in paths:
                path.rename(self.__archive / path.name)

        return tuple(dataframes)


    @typeguard.typechecked
    def process_file(
            self,
            path: pathlib.Path,
            encoding: typing.Optional[str] = None
        ) -> polars.DataFrame:
        raise NotImplementedError("Subclasses must implement this method.")


    @typeguard.typechecked
    def detect_encoding(
            self,
            path: pathlib.Path
        ) -> str:
        with open(path, "rb") as file:
            return chardet.detect(file.read())["encoding"]