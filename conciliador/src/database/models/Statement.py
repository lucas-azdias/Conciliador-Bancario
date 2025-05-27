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


    # Relationships
    statement_entries: sqlalchemy.orm.Mapped[typing.List["StatementEntry"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement",
        viewonly = True
    )