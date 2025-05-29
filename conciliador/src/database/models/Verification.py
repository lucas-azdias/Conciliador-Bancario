import datetime
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class Verification(BaseModel.BaseModel):

    # Table name
    __tablename__ = "verification"


    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        unique = True,
        nullable = False,
        autoincrement = True
    )
    date: sqlalchemy.orm.Mapped[datetime.date] = sqlalchemy.orm.mapped_column(
        unique = True,
        nullable = False
    )
    type: sqlalchemy.orm.Mapped[typing.Optional[typing.List[str]]] = sqlalchemy.orm.mapped_column(
        sqlalchemy.JSON,
        nullable = False
    )
    verified_on: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(
        default = datetime.datetime.now,
        nullable = False
    )
    is_verified: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(
        default = False,
        nullable = False
    )


    # Computed columns
    @sqlalchemy.ext.hybrid.hybrid_property
    def finishers_value(self) -> int:
        return sum(finisher.value for finisher in self.finishers)

    @sqlalchemy.ext.hybrid.hybrid_property
    def statement_entries_value(self) -> int:
        return sum(statement_entry.value for statement_entry in self.statement_entries)


    # Relationships
    finishers: sqlalchemy.orm.Mapped[typing.List["Finisher"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "verification"
    )
    statement_entries: sqlalchemy.orm.Mapped[typing.List["StatementEntry"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "verification"
    )