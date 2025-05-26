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
        nullable = False,
        autoincrement = True
    )
    date: sqlalchemy.orm.Mapped[datetime.date]
    name: sqlalchemy.orm.Mapped[str]
    value: sqlalchemy.orm.Mapped[int]

    # Relationships
    finishers: sqlalchemy.orm.Mapped[typing.List["Finisher"]] = sqlalchemy.orm.relationship( # type: ignore
        secondary = "statement_finisher_link",
        back_populates = "statement",
        viewonly = True
    )