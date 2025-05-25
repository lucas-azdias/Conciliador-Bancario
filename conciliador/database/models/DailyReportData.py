import sqlalchemy
import sqlalchemy.orm
import typing

from .. import BaseModel


class DailyReportData(BaseModel.BaseModel):

    # Table name
    __tablename__ = "daily_report_data"

    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        nullable = False,
        autoincrement = True
    )
    report_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("report.id")
    )
    pix: sqlalchemy.orm.Mapped[int]
    transfer: sqlalchemy.orm.Mapped[int]

    # Relationships
    reports: sqlalchemy.orm.Mapped[typing.List["Report"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "daily_report_data"
    )
    daily_datas: sqlalchemy.orm.Mapped[typing.List["DailyData"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "daily_report_data"
    )
    deposits: sqlalchemy.orm.Mapped[typing.List["Deposit"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "daily_report_data",
        cascade = "all, delete-orphan"
    )
    cards: sqlalchemy.orm.Mapped[typing.List["Card"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "daily_report_data",
        cascade = "all, delete-orphan"
    )