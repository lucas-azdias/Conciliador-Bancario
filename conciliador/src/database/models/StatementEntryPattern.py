import sqlalchemy
import sqlalchemy.orm
import typeguard

from .. import BaseModel


@typeguard.typechecked
class StatementEntryPattern(BaseModel.BaseModel):

    # Table name
    __tablename__ = "statement_entry_pattern"


    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        unique = True,
        nullable = False,
        autoincrement = True
    )
    type_id: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("type.id"),
        nullable = True
    )
    pattern: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        nullable = True
    )
    value_pattern: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        nullable = False
    )


    # Constraints
    __table_args__ = (
        sqlalchemy.UniqueConstraint("pattern", "value_pattern", name = "unique_pattern_value_pattern"),
    )


    # Relationships
    type: sqlalchemy.orm.Mapped["Type"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "statement_entry_patterns"
    )