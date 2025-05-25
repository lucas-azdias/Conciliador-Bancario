import datetime
import sqlalchemy
import sqlalchemy.orm
import typing

from .. import BaseModel


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
    daily_bank_datas: sqlalchemy.orm.Mapped[typing.List["DailyBankData"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement",
        cascade = "all, delete-orphan"
    )