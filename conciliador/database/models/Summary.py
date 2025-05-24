import datetime
import sqlalchemy
import sqlalchemy.orm
import typing

from .. import BaseModel


class Summary(BaseModel.BaseModel):

    # Table name
    __tablename__ = "summary"

    # Columns
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key = True, nullable = False, autoincrement = True)
    statement_data_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("statement_data.id")
    )
    report_data_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("report_data.id")
    )
    date: sqlalchemy.orm.Mapped[datetime.date]

    # Relationships
    statement_datas: sqlalchemy.orm.Mapped[typing.List["StatementData"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "summary"
    )
    report_datas: sqlalchemy.orm.Mapped[typing.List["ReportData"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "summary"
    )