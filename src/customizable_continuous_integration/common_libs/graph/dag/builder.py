"""This module defines functions manipulating DAG

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  29/03/2025   Ryan, Gao       Initial creation
"""

from customizable_continuous_integration.common_libs.graph.dag.entity import DAG, DAGNodeInterface

def build_dag(dag_id: str, raw_entities: list[DAGNodeInterface], external_completed_dependencies: set[str]) -> DAG:
    """
    Build a DAG from a list of raw entities.

    Args:
        dag_id (str): The ID of the DAG.
        raw_entities (list[DAGNodeInterface]): A list of raw entities.

    Returns:
        DAG: A DAG object.
    """
    dag = DAG(dag_id, raw_entities)
    dag.normalize_reverse_relationship()
    dag.mark_nodes_external_requisites(external_completed_dependencies)
    return dag