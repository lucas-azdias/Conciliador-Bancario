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
            infolder: pathlib.Path,
            outfolder: pathlib.Path,
            encoding: typing.Optional[str] = None
        ) -> None:
        self.__path_filter: str = path_filter
        self.__infolder: pathlib.Path = infolder
        self.__outfolder: pathlib.Path = outfolder
        self.__encoding: typing.Optional[str] = encoding


    @typeguard.typechecked
    def process(self) -> None:
        paths: typing.List[pathlib.Path] = list()
        datas: typing.List[polars.DataFrame] = list()

        for path in self.__infolder.rglob(self.__path_filter):
            try:
                data = self.process_file(path, self.__detect_encoding(path))
            except Exception as e:
                Exception(f"Error processing file \"{path}\": {e}")
            else:
                paths.append(path)
                datas.append(data)

        # Archive files
        self.__outfolder.mkdir(parents = True, exist_ok = True)
        for path in paths:
            path.rename(self.__outfolder / path.name)


    @typeguard.typechecked
    def process_file(
            self,
            path: pathlib.Path,
            encoding: str
        ) -> polars.DataFrame:
        raise NotImplementedError("Subclasses must implement this method.")


    @typeguard.typechecked
    def __detect_encoding(
            self,
            path: pathlib.Path
        ) -> str:
        if self.__encoding:
            return self.__encoding
        with open(path, "rb") as file:
            return chardet.detect(file.read())["encoding"]