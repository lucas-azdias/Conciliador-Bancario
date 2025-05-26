import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class StatementFinisherLink(BaseModel.BaseModel):

    # Table name
    __tablename__ = "statement_finisher_link"

    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        nullable = False,
        autoincrement = True
    )
    statement_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("statement.id"),
        nullable = False
    )
    finisher_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("finisher.id"),
        nullable = False
    )

    # Relationships
    statements: sqlalchemy.orm.Mapped[typing.List["Statement"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement_finisher_link"
    )
    finishers: sqlalchemy.orm.Mapped[typing.List["Finishers"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement_finisher_link"
    )