import datetime
import sqlalchemy
import sqlalchemy.orm
import typing

from .. import BaseModel


class DailyBankData(BaseModel.BaseModel):

    # Table name
    __tablename__ = "daily_bank_data"

    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        nullable = False,
        autoincrement = True
    )
    statement_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("statement.id")
    )
    date: sqlalchemy.orm.Mapped[datetime.date]
    name = sqlalchemy.orm.Mapped[str]
    value = sqlalchemy.orm.Mapped[int]

    # Relationships
    statements: sqlalchemy.orm.Mapped[typing.List["Statement"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "daily_bank_data"
    )
    daily_datas: sqlalchemy.orm.Mapped[typing.List["DailyData"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "daily_bank_data"
    )