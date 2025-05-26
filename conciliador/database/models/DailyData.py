import datetime
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class DailyData(BaseModel.BaseModel):

    # Table name
    __tablename__ = "daily_datas"

    # Columns
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key = True, nullable = False, autoincrement = True)
    daily_bank_data_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("daily_bank_data.id")
    )
    daily_report_data_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("daily_report_data.id")
    )
    date: sqlalchemy.orm.Mapped[datetime.date]

    # Relationships
    daily_bank_datas: sqlalchemy.orm.Mapped[typing.List["DailyBankData"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "daily_datas"
    )
    daily_report_datas: sqlalchemy.orm.Mapped[typing.List["DailyReportData"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "daily_datas"
    )