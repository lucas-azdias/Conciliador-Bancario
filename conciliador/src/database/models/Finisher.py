import datetime
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class Finisher(BaseModel.BaseModel):

    # Table name
    __tablename__ = "finisher"


    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        unique = True,
        nullable = False,
        autoincrement = True
    )
    report_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("report.id"),
        nullable = False
    )
    type_id: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("type.id"),
        nullable = True
    )
    verification_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("verification.id"),
        nullable = True
    )
    name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        nullable = False
    )
    value: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        nullable = False
    )
    payment_date: sqlalchemy.orm.Mapped[datetime.date] = sqlalchemy.orm.mapped_column(
        nullable = True
    )
    payment_value: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        nullable = True
    )


    # Constraints
    __table_args__ = (
        sqlalchemy.UniqueConstraint("report_id", "name", name = "unique_report_id_name"),
    )


    # Relationships
    report: sqlalchemy.orm.Mapped["Report"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "finishers"
    )
    type: sqlalchemy.orm.Mapped["Type"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "finishers"
    )
    verification: sqlalchemy.orm.Mapped["Verification"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "finishers"
    )