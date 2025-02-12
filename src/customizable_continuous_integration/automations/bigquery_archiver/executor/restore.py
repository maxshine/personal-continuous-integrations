"""This module hosts retore dataset objects action"""

import logging
import typing
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import google.cloud.bigquery

from customizable_continuous_integration.automations.bigquery_archiver.entity.archive_entities import (
    BigqueryArchivedDatasetEntity,
    BigqueryArchiveTableEntity,
    BigqueryArchiveViewEntity,
    BigqueryBaseArchiveEntity,
)
from customizable_continuous_integration.automations.bigquery_archiver.executor.fetch import BaseExecutor


class RestoreBigqueryDatasetExecutor(BaseExecutor):
    def __init__(
        self,
        bigquery_archived_dataset_config: dict,
        restore_config: dict,
        logger: logging.Logger = None,
        bigquery_client: google.cloud.bigquery.Client = None,
    ):
        self.bigquery_archived_dataset_entity = BigqueryArchivedDatasetEntity.model_validate(bigquery_archived_dataset_config)
        self.bigquery_archived_dataset_entity.populate_sub_restore_info()
        self.restore_config = restore_config
        if not logger:
            logger = logging.getLogger(__class__.__name__)
        self.logger = logger
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.bigquery_archived_dataset_entity.project_id)
        self.bigquery_client = bigquery_client

    def load_single_entity(self, entity: BigqueryBaseArchiveEntity) -> typing.Any:
        if type(entity) is BigqueryArchiveTableEntity or type(entity) is BigqueryArchiveViewEntity:
            entity.load_self(self.bigquery_client)
            return True
        self.logger.warning(f"restore {entity.identity} is not supported type {type(entity)}")
        return False

    def restore_single_entity(self, entity: BigqueryBaseArchiveEntity) -> typing.Any:
        if type(entity) is BigqueryArchiveTableEntity or type(entity) is BigqueryArchiveViewEntity:
            entity.restore_self(self.bigquery_client)
            return True
        self.logger.warning(f"restore {entity.identity} is not supported type {type(entity)}")
        return False

    def execute(self) -> BigqueryArchivedDatasetEntity:
        self.logger.info(f"Retoring entities in the dataset {self.bigquery_archived_dataset_entity.fully_qualified_identity}")

        task_requests = {}
        failed_tasks_results = {}
        concurrency = self.restore_config.get("concurrency", 1)
        continue_on_failure = self.restore_config.get("continue_on_failure", False)

        self.bigquery_archived_dataset_entity.restore_self(self.bigquery_client, self.restore_config)
        # with ThreadPoolExecutor(max_workers=concurrency) as executor:
        #     for idx, table_entity in enumerate(self.bigquery_archived_dataset_entity.tables):
        #         task_req = table_entity
        #         task_requests[executor.submit(self.restore_single_entity, task_req)] = task_req
        #     for idx, view_entity in enumerate(self.bigquery_archived_dataset_entity.views):
        #         task_req = view_entity
        #         task_requests[executor.submit(self.restore_single_entity, task_req)] = task_req
        #     for completed_task in as_completed(task_requests.keys()):
        #         completed_task_req = task_requests[completed_task]
        #         try:
        #             ret = completed_task.result()
        #             if ret:
        #                 self.logger.info(f"{completed_task_req.entity_type} {completed_task_req.identity} Archive Result: {ret}")
        #             elif continue_on_failure:
        #                 self.logger.error(f"{completed_task_req.entity_type} {completed_task_req.identity} Archive FAILED: {ret}, execution will be continued")
        #                 failed_tasks_results[completed_task_req.identity] = ret
        #             else:
        #                 self.logger.error(f"{completed_task_req.entity_type} {completed_task_req.identity} Archive FAILED: {ret}, execution will be stopped")
        #                 executor.shutdown(wait=False, cancel_futures=True)
        #                 exit(1)
        #         except Exception as e:
        #             self.logger.error(f"{completed_task_req.entity_type} {completed_task_req.identity} FAILED with exception: {e}, execution will be stopped")
        #             executor.shutdown(wait=False, cancel_futures=True)
        #             exit(1)
        if failed_tasks_results:
            self.logger.error(f"These archive processes FAILED: {list(failed_tasks_results.keys())}")
            exit(1)
        return self.bigquery_archived_dataset_entity
