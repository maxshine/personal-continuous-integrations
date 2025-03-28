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
        self.dependents: set[str] = set()
        self.requisites: set[str] = self._internal_entity.dag_dependencies()
        self.completed_requisites: set[str] = set()

    def dag_dependencies(self) -> set[str]:
        return self.dependents

    def dag_key(self) -> str:
        return self._internal_entity.dag_key()

    def raw_entity(self) -> DAGNodeInterface:
        return self._internal_entity

    def add_dependent(self, dependent: str) -> None:
        self.dependents.add(dependent)

    def add_requisite(self, requisite: str) -> None:
        self.requisites.add(requisite)

    def complete_requisite(self, requisite: str) -> None:
        if requisite not in self.requisites:
            raise ValueError(f"Requisite {requisite} is not a prerequisite of this node.")
        self.completed_requisites.add(requisite)

    def is_ready(self) -> bool:
        return self.completed_requisites == self.requisites

    def mark_external_requisites(self, dag_node_keys: set[str], external_node_keys: set[str]) -> None:
        self.completed_requisites = self.requisites - dag_node_keys - external_node_keys


class DAG(object):
    def __init__(self, dag_identity: str, raw_entities: list[DAGNodeInterface]) -> None:
        self._dag_identity = dag_identity
        self._nodes: dict[str, DAGNode] = {}
        self._completed_nodes: set[DAGNode] = set()
        self._lock = threading.Lock()
        for raw_entity in raw_entities:
            node = DAGNode(raw_entity)
            self._nodes[node.dag_key()] = node
        self.normalize_reverse_relationship()

    def normalize_reverse_relationship(self) -> None:
        for node in self._nodes.values():
            for dependency in node.requisites:
                if dependency not in self._nodes:
                    continue
                self._nodes[dependency].add_dependent(node.dag_key())

    def add_node(self, raw_node: DAGNodeInterface) -> None:
        with self._lock:
            node = DAGNode(raw_node)
            if node.dag_key() in self._nodes:
                raise ValueError(f"Node {node.dag_key()} already exists in the DAG.")
            self._nodes[node.dag_key()] = node
            self.normalize_reverse_relationship()

    def get_node(self, node_key: str) -> DAGNode:
        with self._lock:
            return self._nodes.get(node_key, None)

    def complete_node(self, node_key: str) -> list[DAGNode]:
        ret = []
        with self._lock:
            node = self.get_node(node_key)
            if node:
                self._completed_nodes.add(node)
            for dependent in node.dependents:
                if dependent in self._nodes:
                    self._nodes[dependent].complete_requisite(node_key)
                    if self._nodes[dependent].is_ready():
                        ret.append(self._nodes[dependent])
        return ret

    def get_ready_nodes(self) -> list[DAGNode]:
        with self._lock:
            return [node for node in self._nodes.values() if node.is_ready() and node not in self._completed_nodes]

    def mark_nodes_external_requisites(self) -> None:
        with self._lock:
            for node in self._nodes.values():
                node.mark_external_requisites(set(self._nodes.keys()), set())
