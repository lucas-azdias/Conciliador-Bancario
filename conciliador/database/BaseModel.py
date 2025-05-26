import sqlalchemy.orm
import typeguard


@typeguard.typechecked
class BaseModel(sqlalchemy.orm.DeclarativeBase):

    def __repr__(self) -> str:
        values = ", ".join(
            f"{col}={getattr(self, col)!r}" for col in self.__table__.columns.keys()
        )
        return f"{self.__class__.__name__}" + f"({values})" if values else ""