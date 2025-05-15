import abc
import chardet
import pathlib
import polars
import typeguard
import typing


class Loader(abc.ABC):

    @typeguard.typechecked
    def process_files(
            self,
            input: pathlib.Path,
            folder_filter: typing.Optional[str] = "*",
            encoding: typing.Optional[str] = None
        ) -> typing.Tuple[polars.DataFrame]:
        paths: typing.List[pathlib.Path] = list()
        dataframes: typing.List[polars.DataFrame] = list()

        if input.is_dir():
            paths.extend(input.rglob(folder_filter))
        elif input.is_file():
            paths.append(input)
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

        return tuple(dataframes)


    @typeguard.typechecked
    def process_file(
            self,
            path: pathlib.Path,
            encoding: typing.Optional[str] = None
        ) -> polars.DataFrame:
        raise NotImplementedError("Subclasses must implement this method.")


    @typeguard.typechecked
    def archive_files(
        self,
        paths: typing.Iterable[pathlib.Path],
        archive: pathlib.Path,
        can_overwrite_archive: bool = False
    ) -> None:
        if not archive:
            raise Exception("No archive folder was given.")

        archive.mkdir(parents = True, exist_ok = True)

        for path in paths:
            destination = archive / path.name

            # Skip if file exists and overwriting is not allowed
            if destination.exists() and not can_overwrite_archive:
                continue

            # Remove the existing file
            if destination.exists():
                destination.unlink()

            path.rename(destination)


    @typeguard.typechecked
    def archive_file(
        self,
        path: pathlib.Path,
        archive: pathlib.Path,
        can_overwrite_archive = False
    ) -> None:
        self.archive_files(
            [path],
            archive,
            can_overwrite_archive = True
        )


    @typeguard.typechecked
    def detect_encoding(
            self,
            path: pathlib.Path
        ) -> str:
        with open(path, "rb") as file:
            return chardet.detect(file.read())["encoding"]