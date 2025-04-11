"""This module defines archived external table entity classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  11/04/2025   Ryan, Gao       Initial creation
"""

import base64
import json
import pickle
import typing

import fsspec
import google.cloud.bigquery.table
from typing_extensions import Self

from customizable_continuous_integration.automations.bigquery_archiver.entity.base import BigqueryBaseArchiveEntity, BigquerySchemaFieldEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.bigquery_metadata import BigqueryPartitionConfig, BigqueryTableMetadata


class BigqueryArchiveGenericExternalTableEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryTableMetadata
    schema_fields: list[BigquerySchemaFieldEntity] = []
    data_archive_format: str = google.cloud.bigquery.job.DestinationFormat.AVRO
    data_compression: str = google.cloud.bigquery.job.Compression.DEFLATE
    partition_config: BigqueryPartitionConfig | None = None
    b64encoded_external_data_configuration: bytes = b""
    _external_data_configuration: google.cloud.bigquery.ExternalConfig | None = None

    @property
    def entity_type(self) -> str:
        return "external_table"

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/external_table={self.identity}/external_table.json"

    @property
    def data_serialized_path(self):
        return f"{self.gcs_prefix}/external_table={self.identity}/data"

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        external_table = bigquery_client.get_table(self.fully_qualified_identity)
        self.schema_fields = [BigquerySchemaFieldEntity.from_dict(f.to_api_repr()) for f in external_table.schema]
        self.bigquery_metadata.description = external_table.description
        self.b64encoded_external_data_configuration = base64.standard_b64encode(pickle.dumps(external_table.external_data_configuration))
        partition_config = None
        if external_table.time_partitioning:
            partition_config = BigqueryPartitionConfig(
                partition_type=external_table.time_partitioning.type_,
                partition_field=external_table.time_partitioning.field,
                partition_expiration_ms=external_table.time_partitioning.expiration_ms or 0,
                partition_require_filter=external_table.time_partitioning.require_partition_filter or False,
                partition_category="TIME",
            )
        elif external_table.range_partitioning:
            partition_config = BigqueryPartitionConfig(
                partition_type="",
                partition_field=external_table.range_partitioning.field,
                partition_expiration_ms=0,
                partition_require_filter=False,
                partition_category="RANGE",
                partition_range=[
                    external_table.range_partitioning.range_.start,
                    external_table.range_partitioning.range_.end,
                    external_table.range_partitioning.range_.interval,
                ],
            )
        self.partition_config = partition_config

    def archive_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, archive_config: dict = None) -> typing.Any:
        self.is_archived = True
        self.actual_archive_metadata_path = self.metadata_serialized_path
        self.actual_archive_data_path = self.data_serialized_path
        with fsspec.open(self.metadata_serialized_path, "w") as f:
            f.write(self.model_dump_json(indent=2, exclude={"_external_data_configuration"}))

    def load_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> Self:
        with fsspec.open(self.metadata_serialized_path, "r") as f:
            loaded_model = self.model_validate(json.load(f))
            for k in loaded_model.model_fields:
                if k in BigqueryArchiveGenericExternalTableEntity.model_fields:
                    setattr(self, k, getattr(loaded_model, k))
        self._external_data_configuration = pickle.loads(base64.standard_b64decode(self.b64encoded_external_data_configuration))

    def restore_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, restore_config: dict = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        if not restore_config:
            restore_config = {}
        fully_qualified_identity = self.fully_qualified_identity
        if self.destination_gcp_project_id and self.destination_bigquery_dataset:
            fully_qualified_identity = f"{self.destination_gcp_project_id}.{self.destination_bigquery_dataset}.{self.identity}"
        if restore_config.get("skip_restore", {}).get(self.identity, False):
            print(f"Skip restoring {self.entity_type} {fully_qualified_identity}")
            return
        if restore_config.get("overwrite_existing", False):
            bigquery_client.delete_table(fully_qualified_identity, not_found_ok=True)
        target_table = google.cloud.bigquery.Table(
            fully_qualified_identity, schema=[f.to_bigquery_schema_field() for f in self.schema_fields] if self.schema_fields else None
        )
        target_table.external_data_configuration = pickle.loads(base64.standard_b64decode(self.b64encoded_external_data_configuration))
        target_table.labels = self.bigquery_metadata.labels
        target_table.description = self.bigquery_metadata.description
        target_table.time_partitioning = (
            self.partition_config.to_bigquery_time_partitioning()
            if self.partition_config and self.partition_config.partition_category == "TIME"
            else None
        )
        target_table.range_partitioning = (
            self.partition_config.to_bigquery_range_partitioning()
            if self.partition_config and self.partition_config.partition_category == "RANGE"
            else None
        )
        if restore_config.get("attach_archive_ts_to_label", True):
            target_table.labels["archive_ts"] = self.archived_datetime_str
        table = bigquery_client.create_table(target_table)
        return table
