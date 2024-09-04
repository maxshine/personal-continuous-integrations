"""This module defines common collection classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
"""

import collections
import copy
import typing

ImmutableDictKeyType = typing.TypeVar("ImmutableDictKeyType")
ImmutableDictValueType = typing.TypeVar("ImmutableDictValueType")


class ImmutableDictWrapper(collections.abc.Mapping[ImmutableDictKeyType, ImmutableDictValueType]):
    """An internal dictionary wrapper with immutable data."""

    def __init__(self, data: dict[ImmutableDictKeyType, ImmutableDictValueType]):
        self.__data = data

    def __getitem__(self, key: ImmutableDictKeyType) -> ImmutableDictValueType:
        return self.__data[key]

    def __len__(self) -> int:
        return len(self.__data)

    def __iter__(self) -> typing.Iterator[ImmutableDictKeyType]:
        return iter(self.__data)

    def __contains__(self, key: object) -> bool:
        return key in self.__data

    @property
    def data(self) -> dict[ImmutableDictKeyType, ImmutableDictValueType]:
        return copy.deepcopy(self.__data)
