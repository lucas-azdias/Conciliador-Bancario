import datetime
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class Statement(BaseModel.BaseModel):

    # Table name
    __tablename__ = "statement"


    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        unique = True,
        nullable = False,
        autoincrement = True
    )
    date: sqlalchemy.orm.Mapped[datetime.date] = sqlalchemy.orm.mapped_column(
        unique = True,
        nullable = False
    )


    # Computed columns
    @property
    def str_date(self) -> str:
        return self.date.isoformat()

    @str_date.setter
    def str_date(self, value: datetime.date | str) -> None:
        if isinstance(value, str):
            self.date = datetime.date.fromisoformat(value)
        else:
            self.date = value


    # Relationships
    statement_entries: sqlalchemy.orm.Mapped[typing.List["StatementEntry"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement",
        cascade = "all, delete-orphan"
    )