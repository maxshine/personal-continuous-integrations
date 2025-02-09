"""This module defines archived entity classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  08/02/2025   Ryan, Gao       Initial creation
"""

import datetime
import typing

import google.cloud.bigquery.table
import pydantic
from typing_extensions import Self

from customizable_continuous_integration.automations.bigquery_archiver.entity.bigquery_metadata import (
    BigqueryBaseMetadata,
    BigqueryDatasetMetadata,
    BigqueryPartitionConfig,
    BigqueryTableMetadata,
    BigqueryViewMetadata,
)


class BigquerySchemaFieldEntity(pydantic.BaseModel):
    name: str
    type: str
    mode: str = "NULLABLE"
    description: str | None = None
    default_value_expression: str | None = None
    fields: tuple[Self] | None = None
    is_nullable: bool = True

    @classmethod
    def from_dict(cls, data_dict: dict) -> Self:
        fields_dict = {k: v for k, v in data_dict.items() if k in BigquerySchemaFieldEntity.model_fields}
        return cls(**fields_dict)


class BigqueryBaseArchiveEntity(pydantic.BaseModel):
    bigquery_metadata: BigqueryBaseMetadata
    gcs_prefix: str
    archived_datetime: datetime.datetime
    is_archived: bool = False

    @property
    def is_archive_completed(self) -> bool:
        return self.is_archived

    @property
    def archived_datetime_str(self) -> str:
        return self.archived_datetime.strftime("%Y%m%d%H%M%S")

    @property
    def real_archive_identity(self) -> str:
        return self.bigquery_metadata.identity

    @property
    def project_id(self) -> str:
        return self.bigquery_metadata.project_id

    @property
    def dataset(self) -> str:
        return self.bigquery_metadata.dataset

    @property
    def identity(self) -> str:
        return self.bigquery_metadata.identity

    @property
    def fully_qualified_identity(self) -> str:
        return f"{self.project_id}.{self.dataset}.{self.identity}" if self.dataset else f"{self.project_id}.{self.identity}"

    def from_dataset_reference(self, dataset_reference: str):
        pass

    def do_archive(self):
        raise NotImplementedError("Please implement me to archive myself")

    def do_restore(self):
        raise NotImplementedError("Please implement me to restore myself")


class BigqueryArchivedTableEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryTableMetadata
    schema_fields: list[BigquerySchemaFieldEntity] = []
    destination_gcp_project_id: str | None = None
    destination_bigquery_dataset: str | None = None

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/table={self.identity}/archive_ts={self.archived_datetime_str}/table.json"

    @property
    def data_serialized_path(self):
        return f"{self.gcs_prefix}/table={self.identity}/archive_ts={self.archived_datetime_str}/data"

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        table = bigquery_client.get_table(self.fully_qualified_identity)
        self.schema_fields = [BigquerySchemaFieldEntity.from_dict(f.to_api_repr()) for f in table.schema]
        self.bigquery_metadata.description = table.description


class BigqueryArchivedViewEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryViewMetadata
    schema_fields: list[BigquerySchemaFieldEntity] = []
    destination_gcp_project_id: str | None = None
    destination_bigquery_dataset: str | None = None

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/view={self.identity}/archive_ts={self.archived_datetime_str}/view.json"

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        table = bigquery_client.get_table(self.fully_qualified_identity)
        self.schema_fields = [BigquerySchemaFieldEntity.from_dict(f.to_api_repr()) for f in table.schema]
        self.bigquery_metadata.description = table.description
        self.bigquery_metadata.defining_query = table.view_query


class BigqueryArchivedDatasetEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryDatasetMetadata
    tables: list[BigqueryArchivedTableEntity] = []
    views: list[BigqueryArchivedViewEntity] = []
    destination_gcp_project_id: str | None = None
    destination_bigquery_dataset: str | None = None

    @property
    def archive_prefix(self):
        return f"{self.gcs_prefix}/dataset={self.identity}/archive_ts={self.archived_datetime_str}"

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/dataset={self.identity}/archive_ts={self.archived_datetime_str}/dataset.json"

    @classmethod
    def from_dict(cls, data_dict: dict) -> Self:
        d = dict()
        metadata_dict = {k: v for k, v in data_dict.items() if k in BigqueryDatasetMetadata.model_fields}
        fields_dict = {k: v for k, v in data_dict.items() if k in BigqueryBaseArchiveEntity.model_fields}
        d["bigquery_metadata"] = BigqueryDatasetMetadata(**metadata_dict)
        d.update(fields_dict)
        if "archived_datetime" not in d:
            d["archived_datetime"] = datetime.datetime.now()
        return cls(**d)

    def generate_bigquery_archived_table(
        self, bigquery_item: google.cloud.bigquery.table.TableListItem, base_metadata: BigqueryBaseMetadata, **kwargs
    ) -> BigqueryArchivedTableEntity:
        partition_config = None
        if bigquery_item.time_partitioning:
            partition_config = BigqueryPartitionConfig(
                partition_type=bigquery_item.time_partitioning.type_,
                partition_field=bigquery_item.time_partitioning.field,
                partition_expiration_ms=bigquery_item.time_partitioning.expiration_ms or 0,
                partition_require_filter=bigquery_item.time_partitioning.require_partition_filter or False,
            )
        d = base_metadata.model_dump()
        d["partition_config"] = partition_config
        extra_fields = {k: v for k, v in kwargs.items() if k in BigqueryArchivedTableEntity.model_fields}
        extra_fields.update({"bigquery_metadata": BigqueryTableMetadata.from_dict(d)})
        return BigqueryArchivedTableEntity(**extra_fields)

    def generate_bigquery_archived_view(
        self, bigquery_item: google.cloud.bigquery.table.TableListItem, base_metadata: BigqueryBaseMetadata, defining_query: str, **kwargs
    ) -> BigqueryArchivedViewEntity:
        d = base_metadata.model_dump()
        d["defining_query"] = defining_query
        extra_fields = {k: v for k, v in kwargs.items() if k in BigqueryArchivedViewEntity.model_fields}
        extra_fields.update({"bigquery_metadata": BigqueryViewMetadata.from_dict(d)})
        return BigqueryArchivedViewEntity(**extra_fields)

    def generate_bigquery_archived_entity_from_table_item(
        self, bigquery_item: google.cloud.bigquery.table.TableListItem
    ) -> BigqueryArchivedTableEntity | BigqueryArchivedViewEntity | None:
        metadata_obj = BigqueryBaseMetadata.from_dict(self.bigquery_metadata.model_dump())
        metadata_obj.identity = bigquery_item.table_id
        metadata_obj.labels = bigquery_item.labels

        if bigquery_item.table_type == "TABLE":
            return self.generate_bigquery_archived_table(
                bigquery_item,
                metadata_obj,
                archived_datetime=self.archived_datetime,
                gcs_prefix=self.generate_sub_serialization_prefix("table"),
                destination_gcp_project_id=self.destination_gcp_project_id,
                destination_bigquery_dataset=self.destination_bigquery_dataset,
            )
        elif bigquery_item.table_type == "VIEW":
            return self.generate_bigquery_archived_view(
                bigquery_item,
                metadata_obj,
                defining_query="Not populated yet",
                archived_datetime=self.archived_datetime,
                gcs_prefix=self.generate_sub_serialization_prefix("view"),
                destination_gcp_project_id=self.destination_gcp_project_id,
                destination_bigquery_dataset=self.destination_bigquery_dataset,
            )
        return None

    def generate_sub_serialization_prefix(self, sub_type: str) -> str:
        sub_types_map = {
            "table": "tables",
            "view": "views",
        }
        return f"{self.archive_prefix}/{sub_types_map.get(sub_type, 'entities')}"

    def populate_sub_restore_info(self) -> typing.Any:
        for t in self.tables:
            t.destination_gcp_project_id = self.destination_gcp_project_id
            t.destination_bigquery_dataset = self.destination_bigquery_dataset
        for t in self.views:
            t.destination_gcp_project_id = self.destination_gcp_project_id
            t.destination_bigquery_dataset = self.destination_bigquery_dataset
        return None
