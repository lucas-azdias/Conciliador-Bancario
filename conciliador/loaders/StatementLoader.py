import pathlib

from . import Loader


class StatementLoader(Loader):

    def __init__(self) -> None:
        super.__init__()


    def process_file(
            self,
            path: pathlib.Path,
            encoding: str
        ) -> None:
        ...