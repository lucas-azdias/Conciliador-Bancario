import datetime
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class Report(BaseModel.BaseModel):

    # Table name
    __tablename__ = "report"


    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        nullable = False,
        autoincrement = True
    )
    shift: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        nullable = False
    )
    employee: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        nullable = False
    )
    start_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(
        nullable = False
    )
    end_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(
        nullable = False
    )


    # Relationships
    finishers: sqlalchemy.orm.Mapped[typing.List["Finisher"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "report",
        cascade = "all, delete-orphan"
    )