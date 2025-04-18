"""This module hosts restore dataset objects action

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  23/02/2025   Ryan, Gao       Initial creation
  11/04/2025   Ryan, Gao       Add support for external table
  13/04/2025   Ryan, Gao       Support configurable statement replacements
"""

import logging
import typing
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import google.cloud.bigquery

from customizable_continuous_integration.automations.bigquery_archiver.entity.base import BigqueryBaseArchiveEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.dataset import BigqueryArchivedDatasetEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.external import BigqueryArchiveGenericExternalTableEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.routine import (
    BigqueryArchiveFunctionEntity,
    BigqueryArchiveStoredProcedureEntity,
)
from customizable_continuous_integration.automations.bigquery_archiver.entity.table import BigqueryArchiveTableEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.view import (
    BigqueryArchiveMaterializedViewEntity,
    BigqueryArchiveViewEntity,
)
from customizable_continuous_integration.automations.bigquery_archiver.executor.fetch import BaseExecutor
from customizable_continuous_integration.common_libs.graph.dag.builder import build_dag


class RestoreBigqueryDatasetExecutor(BaseExecutor):
    def __init__(
        self,
        bigquery_archived_dataset_config: dict,
        restore_config: dict,
        logger: logging.Logger = None,
        bigquery_client: google.cloud.bigquery.Client = None,
    ):
        self.bigquery_archived_dataset_entity = BigqueryArchivedDatasetEntity.model_validate(bigquery_archived_dataset_config)
        self.bigquery_archived_dataset_entity.populate_sub_restore_info(restore_config=restore_config)
        self.restore_config = restore_config
        if not logger:
            logger = logging.getLogger(__class__.__name__)
        self.logger = logger
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.bigquery_archived_dataset_entity.project_id)
        self.bigquery_client = bigquery_client
        self.archived_entity_metadata_version = "v1"

    def load_single_entity(self, entity: BigqueryBaseArchiveEntity) -> typing.Any:
        if type(entity) is BigqueryArchiveTableEntity or type(entity) is BigqueryArchiveViewEntity:
            entity.load_self(self.bigquery_client)
            return True
        self.logger.warning(f"restore {entity.identity} is not supported type {type(entity)}")
        return False

    def restore_single_entity(self, entity: BigqueryBaseArchiveEntity, restore_config: dict = None) -> typing.Any:
        supported_archive_entity_types = (
            BigqueryArchiveTableEntity,
            BigqueryArchiveViewEntity,
            BigqueryArchiveFunctionEntity,
            BigqueryArchiveStoredProcedureEntity,
            BigqueryArchiveMaterializedViewEntity,
            BigqueryArchiveGenericExternalTableEntity,
        )
        if type(entity) in supported_archive_entity_types:
            if self.archived_entity_metadata_version != entity.metadata_version:
                raise TypeError(
                    f"{entity.identity} metadata version {entity.metadata_version} is not compatible with {self.archived_entity_metadata_version}"
                )
            entity.restore_self(self.bigquery_client, restore_config)
            return True
        self.logger.warning(f"restore {entity.identity} is not supported type {type(entity)}")
        return False

    def execute(self) -> BigqueryArchivedDatasetEntity:

        task_requests = {}
        failed_tasks_results = {}
        concurrency = self.restore_config.get("concurrency", 1)
        continue_on_failure = self.restore_config.get("continue_on_failure", False)

        self.logger.info(f"Restoring dataset {self.bigquery_archived_dataset_entity.fully_qualified_identity} itself")
        self.bigquery_archived_dataset_entity.restore_self(self.bigquery_client, self.restore_config)
        self.logger.info(f"Restoring entities in the dataset {self.bigquery_archived_dataset_entity.fully_qualified_identity}")
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            for idx, table_entity in enumerate(
                self.bigquery_archived_dataset_entity.tables
                + self.bigquery_archived_dataset_entity.external_tables
                + self.bigquery_archived_dataset_entity.user_define_functions
                + self.bigquery_archived_dataset_entity.stored_procedures
            ):
                task_req = (table_entity, self.restore_config)
                task_requests[executor.submit(self.restore_single_entity, *task_req)] = task_req[0]
            for completed_task in as_completed(task_requests.keys()):
                completed_task_req = task_requests[completed_task]
                try:
                    ret = completed_task.result()
                    if ret:
                        self.logger.info(f"{completed_task_req.entity_type} {completed_task_req.identity} Restore Result: {ret}")
                    elif continue_on_failure:
                        self.logger.error(
                            f"{completed_task_req.entity_type} {completed_task_req.identity} Restore FAILED: {ret}, execution will be continued"
                        )
                        failed_tasks_results[completed_task_req.identity] = ret
                    else:
                        self.logger.error(
                            f"{completed_task_req.entity_type} {completed_task_req.identity} Restore FAILED: {ret}, execution will be stopped"
                        )
                        executor.shutdown(wait=False, cancel_futures=True)
                        exit(1)
                except Exception as e:
                    self.logger.error(
                        f"{completed_task_req.entity_type} {completed_task_req.identity} FAILED with exception: {e}, execution will be stopped"
                    )
                    executor.shutdown(wait=False, cancel_futures=True)
                    exit(1)
        # Do dependency restoring
        views_dag = build_dag(
            "view_restore_dag", self.bigquery_archived_dataset_entity.views + self.bigquery_archived_dataset_entity.materialized_views, set()
        )
        task_requests.clear()
        failed_tasks_results.clear()
        ready_nodes = views_dag.get_ready_nodes()
        restoring_nodes = {node.dag_key(): node for node in ready_nodes}
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            for idx, node in enumerate(ready_nodes):
                task_req = (node.raw_entity(), self.restore_config, node.dag_key())
                task_requests[executor.submit(self.restore_single_entity, *task_req[0:2])] = task_req
            while restoring_nodes:
                ready_nodes.clear()
                for completed_task in as_completed(task_requests.keys()):
                    completed_task_req = task_requests[completed_task]
                    del task_requests[completed_task]
                    del restoring_nodes[completed_task_req[2]]
                    try:
                        ret = completed_task.result()
                        if ret:
                            self.logger.info(f"{completed_task_req[0].entity_type} {completed_task_req[0].identity} Restore Result: {ret}")
                            ready_nodes.extend(views_dag.complete_node(completed_task_req[2]))
                        elif continue_on_failure:
                            self.logger.error(
                                f"{completed_task_req[0].entity_type} {completed_task_req[0].identity} Restore FAILED: {ret}, execution will be continued"
                            )
                            failed_tasks_results[completed_task_req[0].identity] = ret
                        else:
                            self.logger.error(
                                f"{completed_task_req[0].entity_type} {completed_task_req[0].identity} Restore FAILED: {ret}, execution will be stopped"
                            )
                            executor.shutdown(wait=False, cancel_futures=True)
                            exit(1)
                    except Exception as e:
                        self.logger.error(
                            f"{completed_task_req[0].entity_type} {completed_task_req[0].identity} FAILED with exception: {e}, execution will be stopped"
                        )
                        executor.shutdown(wait=False, cancel_futures=True)
                        exit(1)
                for idx, node in enumerate(ready_nodes):
                    if node.dag_key() in restoring_nodes:
                        continue
                    restoring_nodes[node.dag_key()] = node
                    task_req = (node.raw_entity(), self.restore_config, node.dag_key())
                    task_requests[executor.submit(self.restore_single_entity, *task_req[0:2])] = task_req
        if failed_tasks_results:
            self.logger.error(f"These restoring processes FAILED: {list(failed_tasks_results.keys())}")
            exit(1)
        return self.bigquery_archived_dataset_entity
