import datetime
import sqlalchemy
import sqlalchemy.orm
import typeguard

from .. import BaseModel


@typeguard.typechecked
class Rate(BaseModel.BaseModel):

    # Table name
    __tablename__ = "rate"


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
    rate: sqlalchemy.orm.Mapped[float] = sqlalchemy.orm.mapped_column(
        nullable = False
    )
    start_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(
        nullable = False
    )


    # Constraints
    __table_args__ = (
        sqlalchemy.UniqueConstraint("start_time", "type_id", name = "unique_start_time_type_id"),
    )


    # Computed columns
    @property
    def str_start_time(self) -> str:
        return self.start_time.isoformat()

    @str_start_time.setter
    def str_start_time(self, value: datetime.datetime | str) -> None:
        if isinstance(value, str):
            self.start_time = datetime.datetime.fromisoformat(value)
        else:
            self.start_time = value


    # Relationships
    type: sqlalchemy.orm.Mapped["Type"] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "rates"
    )