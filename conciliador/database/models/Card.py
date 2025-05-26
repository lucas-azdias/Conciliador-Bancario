import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class Card(BaseModel.BaseModel):

    # Table name
    __tablename__ = "card"

    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        nullable = False,
        autoincrement = True
    )
    daily_report_data_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("daily_report_data.id")
    )
    is_credit: sqlalchemy.orm.Mapped[bool]
    is_prepaid: sqlalchemy.orm.Mapped[bool]
    value: sqlalchemy.orm.Mapped[int]

    # Relationships
    daily_report_datas: sqlalchemy.orm.Mapped[typing.List["ReportData"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "card"
    )