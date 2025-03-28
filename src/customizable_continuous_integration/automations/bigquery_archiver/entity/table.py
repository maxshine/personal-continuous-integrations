"""This module defines archived table entity classes

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
from customizable_continuous_integration.automations.bigquery_archiver.entity.bigquery_metadata import BigqueryPartitionConfig, BigqueryTableMetadata


class BigqueryArchiveTableEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryTableMetadata
    schema_fields: list[BigquerySchemaFieldEntity] = []
    data_archive_format: str = google.cloud.bigquery.job.DestinationFormat.AVRO
    data_compression: str = google.cloud.bigquery.job.Compression.SNAPPY
    partition_config: BigqueryPartitionConfig | None = None

    @property
    def entity_type(self) -> str:
        return "table"

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/table={self.identity}/table.json"

    @property
    def data_serialized_path(self):
        return f"{self.gcs_prefix}/table={self.identity}/data"

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        table = bigquery_client.get_table(self.fully_qualified_identity)
        self.schema_fields = [BigquerySchemaFieldEntity.from_dict(f.to_api_repr()) for f in table.schema]
        self.bigquery_metadata.description = table.description

    def archive_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, archive_config: dict = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        self.is_archived = True
        self.actual_archive_metadata_path = self.metadata_serialized_path
        self.actual_archive_data_path = self.data_serialized_path
        with fsspec.open(self.metadata_serialized_path, "w") as f:
            f.write(self.model_dump_json(indent=2))
        export_job = bigquery_client.extract_table(
            job_id_prefix=f"archive_{self.bigquery_metadata.dataset}_{self.identity}_{self.archived_datetime_str}",
            source=self.fully_qualified_identity,
            destination_uris=[f"{self.data_serialized_path}/*"],
            job_config=google.cloud.bigquery.job.ExtractJobConfig(
                destination_format=self.data_archive_format, compression=self.data_compression, use_avro_logical_types=True
            ),
        )
        ret = export_job.result()
        return ret

    def load_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> Self:
        with fsspec.open(self.metadata_serialized_path, "r") as f:
            loaded_model = self.model_validate(json.load(f))
            for k in loaded_model.model_fields:
                if k in BigqueryArchiveTableEntity.model_fields:
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
        load_job = bigquery_client.load_table_from_uri(
            source_uris=f"{self.data_serialized_path}/*",
            destination=fully_qualified_identity,
            job_id_prefix=f"restore_{self.bigquery_metadata.dataset}_{self.identity}_{self.archived_datetime_str}",
            job_config=google.cloud.bigquery.job.LoadJobConfig(
                source_format=self.data_archive_format,
                schema=[f.to_bigquery_schema_field() for f in self.schema_fields] if self.schema_fields else None,
                destination_table_description=self.bigquery_metadata.description,
                time_partitioning=(self.partition_config.to_bigquery_time_partitioning() if self.partition_config else None),
                use_avro_logical_types=True,
            ),
        )
        load_job.result()
        table = bigquery_client.get_table(fully_qualified_identity)
        table.labels = self.bigquery_metadata.labels
        bigquery_client.update_table(table, ["labels"])
        return table
