import sqlalchemy
import sqlalchemy.orm
import typing

from .. import BaseModel


class Finisher(BaseModel.BaseModel):

    # Table name
    __tablename__ = "finisher"

    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        nullable = False,
        autoincrement = True
    )
    report_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("report.id")
    )
    name: sqlalchemy.orm.Mapped[str]
    value: sqlalchemy.orm.Mapped[int]

    # Relationships
    reports: sqlalchemy.orm.Mapped[typing.List["Report"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "finisher"
    )