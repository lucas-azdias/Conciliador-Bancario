import datetime
import sqlalchemy
import sqlalchemy.orm
import typeguard
import typing

from .. import BaseModel


@typeguard.typechecked
class Report(BaseModel.BaseModel):

    # Table name
    __tablename__ = "report"


    # Columns
    id: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        primary_key = True,
        unique = True,
        nullable = False,
        autoincrement = True
    )
    shift: sqlalchemy.orm.Mapped[int] = sqlalchemy.orm.mapped_column(
        nullable = False
    )
    employee: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        nullable = False
    )
    start_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(
        unique = True,
        nullable = False
    )
    end_time: sqlalchemy.orm.Mapped[datetime.datetime] = sqlalchemy.orm.mapped_column(
        unique = True,
        nullable = False
    )


    # Computed columns
    @property
    def str_start_time(self) -> str:
        return self.start_time.isoformat()

    @property
    def str_end_time(self) -> str:
        return self.end_time.isoformat()

    @str_start_time.setter
    def str_start_time(self, value: datetime.datetime | str) -> None:
        if isinstance(value, str):
            self.start_time = datetime.datetime.fromisoformat(value)
        else:
            self.start_time = value

    @str_end_time.setter
    def str_end_time(self, value: datetime.datetime | str) -> None:
        if isinstance(value, str):
            self.end_time = datetime.datetime.fromisoformat(value)
        else:
            self.end_time = value


    # Relationships
    finishers: sqlalchemy.orm.Mapped[typing.List["Finisher"]] = sqlalchemy.orm.relationship( # type: ignore
        back_populates = "report",
        cascade = "all, delete-orphan"
    )