import abc
import chardet
import pathlib
import typing


class Loader(abc.ABC):

    def __init__(
            self,
            path_filter: str,
            infolder: pathlib.Path,
            outfolder: pathlib.Path,
            encoding: typing.Optional[str] = None
        ) -> None:
        self.__path_filter = path_filter
        self.__infolder = infolder
        self.__outfolder = outfolder
        self.__encoding = encoding


    def process(self) -> None:
        paths: typing.List[pathlib.Path] = []

        try:
            for path in self.__infolder.rglob(self.__path_filter):
                has_processed = self.process_file(path, self.__detect_encoding(path))
                if has_processed:
                    paths.append(path)
        except Exception as e:
            Exception(f"Error processing files: {e}")
        else:
            self.__outfolder.mkdir(parents = True, exist_ok = True)
            for path in paths:
                path.rename(self.__outfolder / path.name)


    def process_file(
            self,
            path: pathlib.Path,
            encoding: str
        ) -> None:
        raise NotImplementedError("Subclasses must implement this method.")


    def __detect_encoding(
            self,
            path: pathlib.Path
        ) -> str:
        if self.__encoding:
            return self.__encoding
        with open(path, "rb") as file:
            return chardet.detect(file.read())["encoding"]