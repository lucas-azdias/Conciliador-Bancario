import typing


T = typing.TypeVar("T")


class UniqueList(typing.Generic[T], typing.List[T]):

    def __init__(
            self,
            iterable: typing.Optional[typing.Iterable[T]] = None
        ) -> None:
        super().__init__()
        if iterable:
            self.extend(iterable)


    def __setitem__(
            self,
            index: typing.SupportsIndex,
            item: T
        ) -> None:
        if item not in self or self[index] == item:
            super().__setitem__(index, item)


    def __add__(
            self,
            other: typing.Iterable
        ) -> typing.Self:
        if isinstance(other, typing.Iterable):
            added = UniqueList(self)
            for item in other:
                added.append(item)
            return added
        return NotImplemented


    def __iadd__(
            self,
            other: typing.Iterable
        ) -> typing.Self:
        if isinstance(other, typing.Iterable):
            self.extend(other)
            return self
        return NotImplemented


    def __sub__(
            self,
            other: typing.Iterable[T]
        ) -> typing.Self:
        if isinstance(other, typing.Iterable):
            return UniqueList([item for item in self if item not in other])
        return NotImplemented


    def __isub__(
            self,
            other: typing.Iterable[T]
        ) -> typing.Self:
        if isinstance(other, typing.Iterable):
            return self - other
        return NotImplemented


    def __mul__(
            self,
            _: typing.SupportsIndex
        ) -> typing.Self:
        return UniqueList(self)


    def __imul__(
            self,
            _: typing.SupportsIndex
        ) -> typing.Self:
        return self


    def __repr__(self) -> str:
        return f"UniqueList{super().__repr__()}"


    def append(
            self,
            object: T
        ) -> None:
        if not object in self:
            super().append(object)


    def extend(
            self,
            iterable: typing.Iterable[T]
        ) -> None:
        for object in iterable:
            if not object in self:
                super().append(object)


    def insert(
            self,
            index: typing.SupportsIndex,
            object: T
        ) -> None:
        if not object in self:
            super().insert(index, object)