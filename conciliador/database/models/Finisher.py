import enum
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class FinisherType(str, enum.Enum):

    # Enum constants
    PIX = "pix"
    TRANSFER = "transfer"
    DEPOSIT = "deposit"
    CARD = "card"
    INSTALLMENT = "installment"


    def determine_type(name: str) -> typing.Self:
        filtered_name = name # TODO make it pass thorugh rules to get final name

        if filtered_name in FinisherType._value2member_map_:
            return FinisherType(filtered_name)
        else
            return None


@typeguard.typechecked
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
        sqlalchemy.ForeignKey("report.id"),
        nullable = False
    )
    name: sqlalchemy.orm.Mapped[str]
    value: sqlalchemy.orm.Mapped[int]
    type: sqlalchemy.orm.Mapped[FinisherType] = sqlalchemy.orm.mapped_column(
        sqlalchemy.Enum(FinisherType)
    )

    # Relationships
    reports: sqlalchemy.orm.Mapped[typing.List["Report"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "finisher"
    )
    statements: sqlalchemy.orm.Mapped[typing.List["Statement"]] = sqlalchemy.orm.relationship( # type: ignore
        secondary = "statement_finisher_link",
        back_populates = "finisher",
        viewonly = True
    )