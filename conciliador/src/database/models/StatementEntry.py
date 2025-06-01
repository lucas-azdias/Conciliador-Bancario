import sqlalchemy
import sqlalchemy.orm
import typeguard

from .. import BaseModel


@typeguard.typechecked
class StatementEntry(BaseModel.BaseModel):

    # Table name
    __tablename__ = "statement_entry"


    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        unique = True,
        nullable = False,
        autoincrement = True
    )
    statement_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("statement.id"),
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


    # Relationships
    statement: sqlalchemy.orm.Mapped["Statement"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement_entries"
    )
    type: sqlalchemy.orm.Mapped["Type"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement_entries"
    )
    verification: sqlalchemy.orm.Mapped["Verification"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement_entries"
    )