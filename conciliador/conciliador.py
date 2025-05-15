import pathlib
import typeguard

from . import Currency
from .database import Database, Schema


class Conciliador():

    @typeguard.typechecked
    def __init__(
            self,
            db_schema_path: pathlib.Path = pathlib.Path("db/db_schema.json"),
            database_path: pathlib.Path = pathlib.Path("db/database.db"),
            currency: str = "USD",
            thousands: str = ",",
            decimals: str = "."
        ) -> None:
        self.__database: Database.Database = Database.Database(Schema.Schema(db_schema_path), database_path = database_path, can_load_schema = True)
        self.__currency: Currency.Currency = Currency.Currency(currency, thousands = thousands, decimals = decimals)


    @typeguard.typechecked
    def load(
            infolder: str,
            outfolder: str
        ) -> None:
        ...


if __name__ == "__main__":
    c = Conciliador()