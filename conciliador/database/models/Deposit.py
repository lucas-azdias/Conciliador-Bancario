import sqlalchemy
import sqlalchemy.orm
import typing

from .. import BaseModel


class Deposit(BaseModel.BaseModel):

    # Table name
    __tablename__ = "deposit"

    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        nullable = False,
        autoincrement = True
    )
    report_data_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("report_data.id")
    )
    value: sqlalchemy.orm.Mapped[int]

    # Relationships
    report_datas: sqlalchemy.orm.Mapped[typing.List["ReportData"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "deposit"
    )