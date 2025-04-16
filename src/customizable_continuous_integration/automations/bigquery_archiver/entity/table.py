"""This module defines archived table entity classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  23/02/2025   Ryan, Gao       Initial creation
  04/04/2025   Ryan, Gao       Set DEFLATE as default compression
  10/04/2025   Ryan, Gao       Add archive timestamp labels; Add skip_restore; Add range partitioning
  16/04/2025   Ryan, Gao       AVRO DATETIME:https://cloud.google.com/bigquery/docs/exporting-data#avro_export_details
"""

import json
import typing
from copy import deepcopy

import fsspec
import google.cloud.bigquery.enums
import google.cloud.bigquery.table
from typing_extensions import Self

from customizable_continuous_integration.automations.bigquery_archiver.entity.base import BigqueryBaseArchiveEntity, BigquerySchemaFieldEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.bigquery_metadata import BigqueryPartitionConfig, BigqueryTableMetadata


class BigqueryArchiveTableEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryTableMetadata
    schema_fields: list[BigquerySchemaFieldEntity] = []
    data_archive_format: str = google.cloud.bigquery.job.DestinationFormat.AVRO
    data_compression: str = google.cloud.bigquery.job.Compression.DEFLATE
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
        partition_config = None
        if table.time_partitioning:
            partition_config = BigqueryPartitionConfig(
                partition_type=table.time_partitioning.type_,
                partition_field=table.time_partitioning.field,
                partition_expiration_ms=table.time_partitioning.expiration_ms or 0,
                partition_require_filter=table.time_partitioning.require_partition_filter or False,
                partition_category="TIME",
            )
        elif table.range_partitioning:
            partition_config = BigqueryPartitionConfig(
                partition_type="",
                partition_field=table.range_partitioning.field,
                partition_expiration_ms=0,
                partition_require_filter=False,
                partition_category="RANGE",
                partition_range=[
                    table.range_partitioning.range_.start,
                    table.range_partitioning.range_.end,
                    table.range_partitioning.range_.interval,
                ],
            )
        self.partition_config = partition_config

    def determine_data_archive_format_compression(self, archive_config: dict) -> None:
        data_format = (
            archive_config.get("table_data_archive_format_mapping", {}).get(self.identity, None)
            or archive_config.get("table_data_archive_format", "avro")
        ).lower()
        data_compression = (
            archive_config.get("table_data_archive_compression_mapping", {}).get(self.identity, None)
            or archive_config.get("table_data_archive_compression", "deflate")
        ).lower()
        if data_format == "parquet":
            self.data_archive_format = google.cloud.bigquery.job.DestinationFormat.PARQUET
        elif data_format == "csv":
            self.data_archive_format = google.cloud.bigquery.job.DestinationFormat.CSV
        else:
            self.data_archive_format = google.cloud.bigquery.job.DestinationFormat.AVRO
        if data_compression == "snappy":
            self.data_compression = google.cloud.bigquery.job.Compression.SNAPPY
        elif data_compression == "deflate":
            self.data_compression = google.cloud.bigquery.job.Compression.DEFLATE
        elif data_compression == "gzip":
            self.data_compression = google.cloud.bigquery.job.Compression.GZIP
        elif data_compression == "zstd":
            self.data_compression = google.cloud.bigquery.job.Compression.ZSTD

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
        if restore_config.get("skip_restore", {}).get(self.identity, False):
            print(f"Skip restoring {self.entity_type} {fully_qualified_identity}")
            return
        self.determine_data_archive_format_compression(restore_config)
        use_stage = (self.data_archive_format == google.cloud.bigquery.job.DestinationFormat.AVRO) and (
            any([True if f.type == google.cloud.bigquery.enums.SqlTypeNames.DATETIME.value else False for f in self.schema_fields])
        )
        stage_table_name = f"{self.destination_gcp_project_id or self.project_id}.{self.destination_bigquery_dataset or self.dataset}.temp_stg_load_{self.identity}_{self.archived_datetime_str}"
        restore_table_schema = [f.to_bigquery_schema_field() for f in self.schema_fields] if self.schema_fields else []
        if use_stage:
            restore_table_schema = []
            saved_schema = deepcopy(self.schema_fields)
            for s in saved_schema:
                if s.type == google.cloud.bigquery.enums.SqlTypeNames.DATETIME.value:
                    s.type = google.cloud.bigquery.enums.SqlTypeNames.STRING.value
                restore_table_schema.append(s.to_bigquery_schema_field())
        if restore_config.get("overwrite_existing", False):
            bigquery_client.delete_table(fully_qualified_identity, not_found_ok=True)
        load_job = bigquery_client.load_table_from_uri(
            source_uris=f"{self.data_serialized_path}/*",
            destination=stage_table_name if use_stage else fully_qualified_identity,
            job_id_prefix=f"restore_{self.bigquery_metadata.dataset}_{self.identity}_{self.archived_datetime_str}",
            job_config=google.cloud.bigquery.job.LoadJobConfig(
                source_format=self.data_archive_format,
                schema=restore_table_schema if self.schema_fields else None,
                destination_table_description=self.bigquery_metadata.description,
                time_partitioning=(
                    self.partition_config.to_bigquery_time_partitioning()
                    if self.partition_config and self.partition_config.partition_category == "TIME" and not use_stage
                    else None
                ),
                range_partitioning=(
                    self.partition_config.to_bigquery_range_partitioning()
                    if self.partition_config and self.partition_config.partition_category == "RANGE" and not use_stage
                    else None
                ),
                use_avro_logical_types=True,
            ),
        )
        load_job.result()
        if use_stage:
            datetime_fields = [f.name for f in self.schema_fields if f.type == google.cloud.bigquery.enums.SqlTypeNames.DATETIME.value]
            cast_datetime_fields = [f"CAST({f} AS DATETIME) AS {f}" for f in datetime_fields]
            partition_clause = ""
            if self.partition_config and self.partition_config.partition_category == "TIME":
                partition_clause = f"PARTITION BY {self.partition_config.partition_field}"
            elif self.partition_config and self.partition_config.partition_category == "RANGE":
                partition_clause = f"PARTITION BY RANGE_BUCKET({self.partition_config.partition_field}, GENERATE_ARRAY({self.partition_config.partition_range[0]}, {self.partition_config.partition_range[1]}, {self.partition_config.partition_range[2]}))"
            create_sql = f"""
                CREATE OR REPLACE TABLE `{self.fully_qualified_identity}` {partition_clause} AS
                SELECT * except({",".join(datetime_fields)}), {",".join(cast_datetime_fields)} FROM `{stage_table_name}`
                """
            bigquery_client.query(create_sql).result()
            bigquery_client.delete_table(f"{stage_table_name}", not_found_ok=True)
        table = bigquery_client.get_table(fully_qualified_identity)
        table.labels = self.bigquery_metadata.labels
        table.description = self.bigquery_metadata.description
        if restore_config.get("attach_archive_ts_to_label", True):
            table.labels["archive_ts"] = self.archived_datetime_str
        bigquery_client.update_table(table, ["labels", "description"])
        return table
