"""This module defines the node entity and node interface for DAG.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  23/03/2025   Ryan, Gao       Initial creation
"""

import abc
import threading


class DAGNodeInterface(abc.ABC):
    def dag_dependencies(self) -> set[str]:
        raise NotImplementedError

    def dag_key(self) -> str:
        raise NotImplementedError


class DAGNode(object):
    def __init__(self, internal_entity: DAGNodeInterface) -> None:
        if type(internal_entity) is not DAGNodeInterface:
            raise ValueError("The internal entity must be an instance of DAGNodeInterface")
        self._internal_entity = internal_entity
        self._internal_lock = threading.Lock()
        self.dependents: set[str] = set()
        self.requisites: set[str] = set()

    def dag_dependencies(self) -> set[str]:
        with self._internal_lock:
            return self.dependents

    def dag_key(self) -> str:
        return self._internal_entity.dag_key()
