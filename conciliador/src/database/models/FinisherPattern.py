import sqlalchemy
import sqlalchemy.orm
import typeguard

from .. import BaseModel


@typeguard.typechecked
class FinisherPattern(BaseModel.BaseModel):

    # Table name
    __tablename__ = "finisher_pattern"


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
        unique = True,
        nullable = False
    )
    payment_interval: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        nullable = True
    )


    # Relationships
    type: sqlalchemy.orm.Mapped["Type"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "finisher_patterns"
    )