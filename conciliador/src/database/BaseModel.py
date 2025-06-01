import sqlalchemy.orm
import typeguard
import typing


@typeguard.typechecked
class BaseModel(sqlalchemy.orm.DeclarativeBase):

    __abstract__ = True


    def __repr__(self) -> str:
        values = ", ".join(
            f"{col}={getattr(self, col)!r}" for col in self.__table__.columns.keys()
        )
        return f"{self.__class__.__name__}" + f"({values})" if values else ""


    def to_dict(self) -> typing.Dict[str, typing.Any]:
        instance_dict = self.__dict__
        instance_dict.pop("_sa_instance_state", None)
        return instance_dict@staticmethod


    @staticmethod
    def get_model(
            table_name: str
        ) -> typing.Type["BaseModel"]:
        for subcls in BaseModel.__subclasses__():
            if hasattr(subcls, "__tablename__") and subcls.__tablename__ == table_name:
                return subcls
        raise ValueError(f"Invalid table name \"{table_name}\" was given.")