import re
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


TYPES_NAMES: typing.Tuple[str, ...] = (
    "cash", "revenue", "usage_and_consumption", "installment", "pix",
    "card.debit.visa", "card.debit.master", "card.debit.elo",
    "card.credit.visa", "card.credit.master", "card.credit.elo", "card.credit.hipercard", "card.credit.amex",
    "income", "outcome",
)


@typeguard.typechecked
class Type(BaseModel.BaseModel):

    # Table name
    __tablename__ = "type"


    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        unique = True,
        nullable = False,
        autoincrement = True
    )
    name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        unique = True,
        nullable = False
    )


    # Relationships
    finishers: sqlalchemy.orm.Mapped[typing.List["Finisher"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "type"
    )
    statement_entries: sqlalchemy.orm.Mapped[typing.List["StatementEntry"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "type"
    )
    rates: sqlalchemy.orm.Mapped[typing.List["Rate"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "type"
    )
    verifications: sqlalchemy.orm.Mapped[typing.List["Verification"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "type"
    )