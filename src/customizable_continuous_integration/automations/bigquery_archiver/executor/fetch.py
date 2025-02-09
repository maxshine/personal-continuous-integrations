"""This module hosts fetch dataset objects action"""

import logging

import google.cloud.bigquery

from customizable_continuous_integration.automations.bigquery_archiver.entity.archive import (
    BigqueryArchivedDatasetEntity,
    BigqueryArchivedTableEntity,
    BigqueryArchivedViewEntity,
)


class BaseExecutor(object):
    def execute(self):
        raise NotImplementedError("Please implement me")


class FetchBigqueryDatasetsExecutor(BaseExecutor):
    def __init__(
        self,
        bigquery_archived_dataset_entity: BigqueryArchivedDatasetEntity,
        logger: logging.Logger = None,
        bigquery_client: google.cloud.bigquery.Client = None,
    ):
        self.bigquery_archived_dataset_entity = bigquery_archived_dataset_entity
        if not logger:
            logger = logging.getLogger(__class__.__name__)
        self.logger = logger
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.bigquery_archived_dataset_entity.project_id)
        self.bigquery_client = bigquery_client

    def execute(self) -> BigqueryArchivedDatasetEntity:
        ds = self.bigquery_client.get_dataset(self.bigquery_archived_dataset_entity.dataset)
        for e in self.bigquery_client.list_tables(dataset=ds):
            self.logger.info(f"Table: {e.table_id}")
            entity = self.bigquery_archived_dataset_entity.generate_bigquery_archived_entity_from_table_item(e)
            if not entity:
                self.logger.warning(f"{e.table_type} {e.table_id} is not supported")
            if type(entity) is BigqueryArchivedTableEntity:
                self.bigquery_archived_dataset_entity.tables.append(entity)
            elif type(entity) is BigqueryArchivedViewEntity:
                self.bigquery_archived_dataset_entity.views.append(entity)
            else:
                self.logger.warning(f"{e.table_type} {e.table_id} is not supported")
        return self.bigquery_archived_dataset_entity
