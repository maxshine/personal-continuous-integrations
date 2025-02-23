"""This module hosts fetch dataset objects action"""

import logging

import google.cloud.bigquery

from customizable_continuous_integration.automations.bigquery_archiver.entity.dataset import BigqueryArchivedDatasetEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.routine import (
    BigqueryArchiveFunctionEntity,
    BigqueryArchiveStoredProcedureEntity,
)
from customizable_continuous_integration.automations.bigquery_archiver.entity.table import BigqueryArchiveTableEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.view import (
    BigqueryArchiveMaterializedViewEntity,
    BigqueryArchiveViewEntity,
)


class BaseExecutor(object):
    def execute(self):
        raise NotImplementedError("Please implement me")


class FetchSourceBigqueryDatasetExecutor(BaseExecutor):
    def __init__(
        self,
        bigquery_archived_dataset_config: dict,
        logger: logging.Logger = None,
        bigquery_client: google.cloud.bigquery.Client = None,
    ):
        self.bigquery_archived_dataset_entity = BigqueryArchivedDatasetEntity.from_dict(bigquery_archived_dataset_config)
        if not logger:
            logger = logging.getLogger(__class__.__name__)
        self.logger = logger
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.bigquery_archived_dataset_entity.project_id)
        self.bigquery_client = bigquery_client

    def execute(self) -> BigqueryArchivedDatasetEntity:
        self.bigquery_archived_dataset_entity.fetch_self(self.bigquery_client)
        ds = self.bigquery_client.get_dataset(self.bigquery_archived_dataset_entity.dataset)
        for e in self.bigquery_client.list_tables(dataset=ds):
            self.logger.info(f"Table: {e.table_id}")
            entity = self.bigquery_archived_dataset_entity.generate_bigquery_archived_entity_from_table_item(e)
            if not entity:
                self.logger.warning(f"{e.table_type} {e.table_id} is not supported")
            if type(entity) is BigqueryArchiveTableEntity:
                entity.fetch_self(self.bigquery_client)
                self.bigquery_archived_dataset_entity.tables.append(entity)
            elif type(entity) is BigqueryArchiveViewEntity:
                entity.fetch_self(self.bigquery_client)
                self.bigquery_archived_dataset_entity.views.append(entity)
            elif type(entity) is BigqueryArchiveMaterializedViewEntity:
                entity.fetch_self(self.bigquery_client)
                self.bigquery_archived_dataset_entity.materialized_views.append(entity)
            else:
                self.logger.warning(f"{e.table_type} {e.table_id} is not supported")
        for e in self.bigquery_client.list_routines(dataset=ds):
            self.logger.info(f"Routine: {e.routine_id}")
            entity = self.bigquery_archived_dataset_entity.generate_bigquery_archived_entity_from_table_item(e)
            if not entity:
                self.logger.warning(f"{e.table_type} {e.table_id} is not supported")
            if type(entity) is BigqueryArchiveFunctionEntity:
                entity.fetch_self(self.bigquery_client)
                self.bigquery_archived_dataset_entity.user_define_functions.append(entity)
            elif type(entity) is BigqueryArchiveStoredProcedureEntity:
                entity.fetch_self(self.bigquery_client)
                self.bigquery_archived_dataset_entity.stored_procedures.append(entity)
            else:
                self.logger.warning(f"{e.table_type} {e.table_id} is not supported")
        return self.bigquery_archived_dataset_entity
