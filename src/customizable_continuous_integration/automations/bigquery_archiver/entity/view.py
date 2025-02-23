"""This module defines archived view entity classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  23/02/2025   Ryan, Gao       Initial creation
"""

import json
import typing

import fsspec
import google.cloud.bigquery.table
from typing_extensions import Self

from customizable_continuous_integration.automations.bigquery_archiver.entity.base import BigqueryBaseArchiveEntity, BigquerySchemaFieldEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.bigquery_metadata import BigqueryPartitionConfig, BigqueryViewMetadata


class BigqueryArchiveViewEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryViewMetadata
    defining_query: str = ""
    schema_fields: list[BigquerySchemaFieldEntity] = []

    @property
    def entity_type(self) -> str:
        return "view"

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/view={self.identity}/archive_ts={self.archived_datetime_str}/view.json"

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        table = bigquery_client.get_table(self.fully_qualified_identity)
        self.schema_fields = [BigquerySchemaFieldEntity.from_dict(f.to_api_repr()) for f in table.schema]
        self.bigquery_metadata.description = table.description
        self.defining_query = table.view_query

    def archive_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, archive_config: dict = None) -> typing.Any:
        self.is_archived = True
        self.actual_archive_metadata_path = self.metadata_serialized_path
        with fsspec.open(self.metadata_serialized_path, "w") as f:
            f.write(self.model_dump_json(indent=2))

    def load_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> Self:
        with fsspec.open(self.metadata_serialized_path, "r") as f:
            loaded_model = self.model_validate(json.load(f))
            for k in loaded_model.model_fields:
                if k in BigqueryViewMetadata.model_fields:
                    setattr(self, k, getattr(loaded_model, k))

    def restore_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, restore_config: dict = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        if not restore_config:
            restore_config = {}
        fully_qualified_identity = self.fully_qualified_identity
        if self.destination_gcp_project_id and self.destination_bigquery_dataset:
            fully_qualified_identity = f"{self.destination_gcp_project_id}.{self.destination_bigquery_dataset}.{self.identity}"
        if restore_config.get("overwrite_existing", False):
            bigquery_client.delete_table(fully_qualified_identity, not_found_ok=True)
        view = google.cloud.bigquery.Table(fully_qualified_identity)
        view.view_query = self.defining_query
        view = bigquery_client.create_table(view, exists_ok=True)
        view.description = self.bigquery_metadata.description
        view.labels = self.bigquery_metadata.labels
        view.schema = [f.to_biguqery_schema_field() for f in self.schema_fields] if self.schema_fields else None
        table = bigquery_client.update_table(view, ["description", "schema", "labels"])
        return table


class BigqueryArchiveMaterializedViewEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryViewMetadata
    enable_refresh: bool = False
    refresh_interval_seconds: int = 1800
    mview_query: str = ""
    partition_config: BigqueryPartitionConfig | None = None
    schema_fields: list[BigquerySchemaFieldEntity] = []

    @property
    def entity_type(self) -> str:
        return "materialized_view"

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/view={self.identity}/archive_ts={self.archived_datetime_str}/materialized_view.json"

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        table = bigquery_client.get_table(self.fully_qualified_identity)
        self.schema_fields = [BigquerySchemaFieldEntity.from_dict(f.to_api_repr()) for f in table.schema]
        self.bigquery_metadata.description = table.description
        self.enable_refresh = table.mview_enable_refresh
        self.refresh_interval_seconds = table.mview_refresh_interval.seconds
        self.mview_query = table.mview_query
        if table.time_partitioning:
            self.partition_config = BigqueryPartitionConfig(
                partition_type=table.time_partitioning.type_,
                partition_field=table.time_partitioning.field,
                partition_expiration_ms=table.time_partitioning.expiration_ms or 0,
                partition_require_filter=table.time_partitioning.require_partition_filter or False,
            )

    def archive_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, archive_config: dict = None) -> typing.Any:
        self.is_archived = True
        self.actual_archive_metadata_path = self.metadata_serialized_path
        with fsspec.open(self.metadata_serialized_path, "w") as f:
            f.write(self.model_dump_json(indent=2))

    def restore_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, restore_config: dict = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        if not restore_config:
            restore_config = {}
        fully_qualified_identity = self.fully_qualified_identity
        if self.destination_gcp_project_id and self.destination_bigquery_dataset:
            fully_qualified_identity = f"{self.destination_gcp_project_id}.{self.destination_bigquery_dataset}.{self.identity}"
        if restore_config.get("overwrite_existing", False):
            bigquery_client.delete_table(fully_qualified_identity, not_found_ok=True)
        stmt = f"""CREATE MATERIALIZED VIEW {fully_qualified_identity}
            OPTIONS (enable_refresh = {self.enable_refresh}, refresh_interval_minutes = {self.refresh_interval_seconds // 60}) 
            AS ({self.mview_query})"""
        job = bigquery_client.query(stmt)
        job.result()
        view = bigquery_client.get_table(fully_qualified_identity)
        view.description = self.bigquery_metadata.description
        view.labels = self.bigquery_metadata.labels
        view.schema = [f.to_biguqery_schema_field() for f in self.schema_fields] if self.schema_fields else None
        table = bigquery_client.update_table(view, ["description", "schema", "labels"])
        return table