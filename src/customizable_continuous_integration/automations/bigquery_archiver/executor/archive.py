"""This module hosts archive dataset objects action

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  23/02/2025   Ryan, Gao       Initial creation
  11/04/2025   Ryan, Gao       Add support for external table
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


class ArchiveSourceBigqueryDatasetExecutor(BaseExecutor):
    def __init__(
        self,
        bigquery_archived_dataset_entity: BigqueryArchivedDatasetEntity,
        archive_config: dict,
        logger: logging.Logger = None,
        bigquery_client: google.cloud.bigquery.Client = None,
    ):
        self.bigquery_archived_dataset_entity = bigquery_archived_dataset_entity
        self.archive_config = archive_config
        if not logger:
            logger = logging.getLogger(__class__.__name__)
        self.logger = logger
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.bigquery_archived_dataset_entity.project_id)
        self.bigquery_client = bigquery_client

    def archive_single_entity(self, entity: BigqueryBaseArchiveEntity) -> typing.Any:
        supported_archive_entity_types = (
            BigqueryArchiveTableEntity,
            BigqueryArchiveViewEntity,
            BigqueryArchiveFunctionEntity,
            BigqueryArchiveStoredProcedureEntity,
            BigqueryArchiveMaterializedViewEntity,
            BigqueryArchiveGenericExternalTableEntity,
        )
        if type(entity) in supported_archive_entity_types:
            entity.archive_self(self.bigquery_client, self.archive_config)
            return True
        self.logger.warning(f"{entity.identity} is not supported type {type(entity)}")
        return False

    def execute(self) -> BigqueryArchivedDatasetEntity:
        self.logger.info(f"Archiving entities in the dataset {self.bigquery_archived_dataset_entity.fully_qualified_identity}")

        task_requests = {}
        failed_tasks_results = {}
        concurrency = self.archive_config.get("concurrency", 1)
        continue_on_failure = self.archive_config.get("continue_on_failure", False)
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            for idx, table_entity in enumerate(self.bigquery_archived_dataset_entity.tables):
                task_req = table_entity
                task_requests[executor.submit(self.archive_single_entity, task_req)] = task_req
            for idx, view_entity in enumerate(self.bigquery_archived_dataset_entity.external_tables):
                task_req = view_entity
                task_requests[executor.submit(self.archive_single_entity, task_req)] = task_req
            for idx, view_entity in enumerate(self.bigquery_archived_dataset_entity.views):
                task_req = view_entity
                task_requests[executor.submit(self.archive_single_entity, task_req)] = task_req
            for idx, view_entity in enumerate(self.bigquery_archived_dataset_entity.materialized_views):
                task_req = view_entity
                task_requests[executor.submit(self.archive_single_entity, task_req)] = task_req
            for idx, view_entity in enumerate(self.bigquery_archived_dataset_entity.user_define_functions):
                task_req = view_entity
                task_requests[executor.submit(self.archive_single_entity, task_req)] = task_req
            for idx, view_entity in enumerate(self.bigquery_archived_dataset_entity.stored_procedures):
                task_req = view_entity
                task_requests[executor.submit(self.archive_single_entity, task_req)] = task_req
            for completed_task in as_completed(task_requests.keys()):
                completed_task_req = task_requests[completed_task]
                try:
                    ret = completed_task.result()
                    if ret:
                        self.logger.info(f"{completed_task_req.entity_type} {completed_task_req.identity} Archive Result: {ret}")
                    elif continue_on_failure:
                        self.logger.error(
                            f"{completed_task_req.entity_type} {completed_task_req.identity} Archive FAILED: {ret}, execution will be continued"
                        )
                        failed_tasks_results[completed_task_req.identity] = ret
                    else:
                        self.logger.error(
                            f"{completed_task_req.entity_type} {completed_task_req.identity} Archive FAILED: {ret}, execution will be stopped"
                        )
                        executor.shutdown(wait=False, cancel_futures=True)
                        exit(1)
                except Exception as e:
                    self.logger.error(
                        f"{completed_task_req.entity_type} {completed_task_req.identity} FAILED with exception: {e}, execution will be stopped"
                    )
                    executor.shutdown(wait=False, cancel_futures=True)
                    exit(1)
        if failed_tasks_results:
            self.logger.error(f"These archive processes FAILED: {list(failed_tasks_results.keys())}")
            exit(1)
        self.bigquery_archived_dataset_entity.is_archived = True
        self.bigquery_archived_dataset_entity.archive_self(self.bigquery_client)
        return self.bigquery_archived_dataset_entity
