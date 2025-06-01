import datetime
import sqlalchemy
import sqlalchemy.ext.hybrid
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
    type_id: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("type.id"),
        nullable = False
    )
    date: sqlalchemy.orm.Mapped[datetime.date] = sqlalchemy.orm.mapped_column(
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


    # Constraints
    __table_args__ = (
        sqlalchemy.UniqueConstraint("type_id", "date", name = "unique_type_id_date"),
    )


    # Computed columns
    @property
    def str_date(self) -> str:
        return self.date.isoformat()

    @str_date.setter
    def str_date(self, value: datetime.date | str) -> None:
        if isinstance(value, str):
            self.date = datetime.date.fromisoformat(value)
        else:
            self.date = value


    # Computed columns
    @sqlalchemy.ext.hybrid.hybrid_property
    def finishers_value(self) -> int:
        return sum(finisher.value for finisher in self.finishers)

    @sqlalchemy.ext.hybrid.hybrid_property
    def statement_entries_value(self) -> int:
        return sum(statement_entry.value for statement_entry in self.statement_entries)


    # Relationships
    type: sqlalchemy.orm.Mapped["Type"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "verifications"
    )
    finishers: sqlalchemy.orm.Mapped[typing.List["Finisher"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "verification"
    )
    statement_entries: sqlalchemy.orm.Mapped[typing.List["StatementEntry"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "verification"
    )