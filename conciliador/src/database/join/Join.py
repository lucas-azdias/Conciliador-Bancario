import sqlalchemy
import typeguard
import typing

from .. import BaseModel
from . import JoinTypeEnum


@typeguard.typechecked
class Join():

    def __init__(
            self,
            left_table_name: str,
            right_table_name: str,
            clause: typing.Callable[[BaseModel.BaseModel, BaseModel.BaseModel], sqlalchemy.ClauseElement],
            type: JoinTypeEnum.JoinTypeEnum = JoinTypeEnum.JoinTypeEnum.INNER
        ):
        self.__left_table_name: str = left_table_name
        self.__right_table_name: str = right_table_name
        self.__clause: typing.Callable[[BaseModel.BaseModel, BaseModel.BaseModel], sqlalchemy.ClauseElement] = clause
        self.__type: JoinTypeEnum.JoinTypeEnum = type


    @property
    def left_table_name(self) -> str:
        return self.__left_table_name


    @property
    def right_table_name(self) -> str:
        return self.__right_table_name


    @property
    def clause(self) -> typing.Callable[[BaseModel.BaseModel, BaseModel.BaseModel], sqlalchemy.ClauseElement]:
        return self.__clause


    @property
    def type(self) -> JoinTypeEnum.JoinTypeEnum:
        return self.__type