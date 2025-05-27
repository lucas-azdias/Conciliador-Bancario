import sqlalchemy
import sqlalchemy.orm
import typeguard

from .. import BaseModel


@typeguard.typechecked
class StatementEntryFinisherLink(BaseModel.BaseModel):

    # Table name
    __tablename__ = "statement_entry_finisher_link"


    # Columns
    statement_entry_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("statement_entry.id"),
        primary_key = True,
        nullable = False
    )
    finisher_id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("finisher.id"),
        primary_key = True,
        nullable = False
    )
    is_verified: sqlalchemy.orm.Mapped[bool] = sqlalchemy.orm.mapped_column(
        default = False,
        nullable = False
    )