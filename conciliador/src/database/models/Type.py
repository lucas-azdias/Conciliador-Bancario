import datetime
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class Type(BaseModel.BaseModel):

    # Table name
    __tablename__ = "type"


    # Columns
    id: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        primary_key = True,
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
    finisher_patterns: sqlalchemy.orm.Mapped[typing.List["FinisherPattern"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "type"
    )
    statement_entry_patterns: sqlalchemy.orm.Mapped[typing.List["StatementEntryPattern"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "type"
    )
    rates: sqlalchemy.orm.Mapped[typing.List["Rate"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "type"
    )
    verifications: sqlalchemy.orm.Mapped[typing.List["Verification"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "type"
    )