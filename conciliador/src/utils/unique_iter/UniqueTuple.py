import typeguard
import typing


T = typing.TypeVar("T")


@typeguard.typechecked
class UniqueTuple(typing.Generic[T], typing.Tuple[T]):

    def __new__(
            cls,
            iterable: typing.Optional[typing.Iterable[T]] = None
        ) -> typing.Self:
        unique_iter = []
        for object in iterable:
            if not object in unique_iter:
                unique_iter.append(object)
        return super().__new__(cls, unique_iter)


    def __sub__(
            self,
            other: typing.Iterable[T]
        ) -> typing.Self:
        result = [item for item in self if item not in other]
        return UniqueTuple(result)


    def __repr__(self) -> str:
        return f"UniqueTuple{super().__repr__()}"