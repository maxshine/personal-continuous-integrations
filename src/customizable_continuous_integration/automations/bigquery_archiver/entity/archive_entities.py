"""This module defines archived entity classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  08/02/2025   Ryan, Gao       Initial creation
"""

import datetime
import json
import typing

import fsspec
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

    def to_biguqery_schema_field(self) -> google.cloud.bigquery.SchemaField:
        schema_field = google.cloud.bigquery.SchemaField(
            self.name,
            self.type,
            mode=self.mode,
            description=self.description,
            default_value_expression=self.default_value_expression,
            fields=[f.to_biguqery_schema_field() for f in self.fields] if self.fields else [],
        )
        return schema_field


class BigqueryBaseArchiveEntity(pydantic.BaseModel):
    bigquery_metadata: BigqueryBaseMetadata
    gcs_prefix: str
    archived_datetime: datetime.datetime
    is_archived: bool = False
    actual_archive_metadata_path: str = ""
    actual_archive_data_path: str = ""
    destination_gcp_project_id: str | None = None
    destination_bigquery_dataset: str | None = None

    @property
    def entity_type(self) -> str:
        return "base_entity"

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
        return f"{self.project_id}.{self.dataset}.{self.identity}"

    def from_dataset_reference(self, dataset_reference: str):
        pass

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client) -> typing.Any:
        raise NotImplementedError("Please implement me to fetch myself")

    def archive_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, archive_config: dict = None) -> typing.Any:
        raise NotImplementedError("Please implement me to fetch myself")

    def load_self(self, bigquery_client: google.cloud.bigquery.client.Client) -> typing.Any:
        raise NotImplementedError("Please implement me to fetch myself")

    def restore_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, _config: dict = None) -> typing.Any:
        raise NotImplementedError("Please implement me to fetch myself")


class BigqueryArchiveTableEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryTableMetadata
    schema_fields: list[BigquerySchemaFieldEntity] = []
    data_archive_format: str = google.cloud.bigquery.job.DestinationFormat.AVRO
    data_compression: str = google.cloud.bigquery.job.Compression.SNAPPY

    @property
    def entity_type(self) -> str:
        return "table"

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/table={self.identity}/archive_ts={self.archived_datetime_str}/table.json"

    @property
    def data_serialized_path(self):
        return f"{self.gcs_prefix}/table={self.identity}/archive_ts={self.archived_datetime_str}/data"

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
                if k in BigqueryArchivedDatasetEntity.model_fields:
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
                schema=[f.to_biguqery_schema_field() for f in self.schema_fields] if self.schema_fields else None,
                destination_table_description=self.bigquery_metadata.description,
                time_partitioning=(
                    self.bigquery_metadata.partition_config.to_bigquery_time_partitioning() if self.bigquery_metadata.partition_config else None
                ),
                use_avro_logical_types=True,
            ),
        )
        load_job.result()
        table = bigquery_client.get_table(fully_qualified_identity)
        table.labels = self.bigquery_metadata.labels
        bigquery_client.update_table(table, ["labels"])
        return table


class BigqueryArchiveViewEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryViewMetadata
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
        self.bigquery_metadata.defining_query = table.view_query

    def archive_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, archive_config: dict = None) -> typing.Any:
        self.is_archived = True
        self.actual_archive_metadata_path = self.metadata_serialized_path
        with fsspec.open(self.metadata_serialized_path, "w") as f:
            f.write(self.model_dump_json(indent=2))

    def load_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> Self:
        with fsspec.open(self.metadata_serialized_path, "r") as f:
            loaded_model = self.model_validate(json.load(f))
            for k in loaded_model.model_fields:
                if k in BigqueryArchivedDatasetEntity.model_fields:
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
        view.view_query = self.bigquery_metadata.defining_query
        view = bigquery_client.create_table(view, exists_ok=True)
        view.description = self.bigquery_metadata.description
        view.labels = self.bigquery_metadata.labels
        view.schema = [f.to_biguqery_schema_field() for f in self.schema_fields] if self.schema_fields else None
        table = bigquery_client.update_table(view, ["description", "schema", "labels"])
        return table


class BigqueryArchiveMaterializedViewEntity(BigqueryArchiveViewEntity):
    enable_refresh: bool = False
    refresh_interval_seconds: int = 1800
    mview_query: str = ""
    partition_config: BigqueryPartitionConfig | None = None

    @property
    def entity_type(self) -> str:
        return "materialized_view"

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/view={self.identity}/archive_ts={self.archived_datetime_str}/view.json"

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        super().fetch_self(bigquery_client)
        table = bigquery_client.get_table(self.fully_qualified_identity)
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


class BigqueryArchiveFunctionEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryBaseMetadata
    body: str = ""
    imported_libraries: list[str] = []
    arguments: list[dict] = []
    language: str = "SQL"
    return_type: str = None

    @property
    def entity_type(self) -> str:
        return "user_defined_function"

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/function={self.identity}/archive_ts={self.archived_datetime_str}/function.json"

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        routine = bigquery_client.get_routine(self.fully_qualified_identity)
        self.body = routine.body
        self.imported_libraries = routine.imported_libraries
        self.arguments = [{"name": arg.name, "data_type": arg.data_type.type_kind.value} for arg in routine.arguments]
        self.return_type = routine.return_type.type_kind.value
        self.language = routine.language
        self.bigquery_metadata.description = routine.description

    def archive_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, archive_config: dict = None) -> typing.Any:
        self.is_archived = True
        self.actual_archive_metadata_path = self.metadata_serialized_path
        with fsspec.open(self.metadata_serialized_path, "w") as f:
            f.write(self.model_dump_json(indent=2))


class BigqueryArchiveStoredProcedureEntity(BigqueryArchiveViewEntity):
    bigquery_metadata: BigqueryBaseMetadata
    body: str = ""
    imported_libraries: list[str] = []
    arguments: list[dict] = []
    language: str = "SQL"
    return_type: str = None

    @property
    def entity_type(self) -> str:
        return "stored_procedure"

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/stored_procedure={self.identity}/archive_ts={self.archived_datetime_str}/stored_procedure.json"

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        routine = bigquery_client.get_routine(self.fully_qualified_identity)
        self.body = routine.body
        self.imported_libraries = routine.imported_libraries
        self.arguments = [{"name": arg.name, "data_type": arg.data_type.type_kind.value} for arg in routine.arguments]
        self.language = routine.language
        self.bigquery_metadata.description = routine.description

    def archive_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, archive_config: dict = None) -> typing.Any:
        self.is_archived = True
        self.actual_archive_metadata_path = self.metadata_serialized_path
        with fsspec.open(self.metadata_serialized_path, "w") as f:
            f.write(self.model_dump_json(indent=2))


class BigqueryArchivedDatasetEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryDatasetMetadata
    tables: list[BigqueryArchiveTableEntity] = []
    views: list[BigqueryArchiveViewEntity] = []
    materialized_views: list[BigqueryArchiveMaterializedViewEntity] = []
    user_define_functions: list[BigqueryArchiveFunctionEntity] = []
    stored_procedures: list[BigqueryArchiveStoredProcedureEntity] = []

    @property
    def fully_qualified_identity(self) -> str:
        return f"{self.project_id}.{self.identity}"

    @property
    def entity_type(self) -> str:
        return "dataset"

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
            d["archived_datetime"] = datetime.datetime.now(tz=datetime.timezone.utc)
        return cls(**d)

    def generate_bigquery_archived_table(
        self, bigquery_item: google.cloud.bigquery.table.TableListItem, base_metadata: BigqueryBaseMetadata, **kwargs
    ) -> BigqueryArchiveTableEntity:
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
        extra_fields = {k: v for k, v in kwargs.items() if k in BigqueryArchiveTableEntity.model_fields}
        extra_fields.update({"bigquery_metadata": BigqueryTableMetadata.from_dict(d)})
        return BigqueryArchiveTableEntity(**extra_fields)

    def generate_bigquery_archived_view(
        self, bigquery_item: google.cloud.bigquery.table.TableListItem, base_metadata: BigqueryBaseMetadata, defining_query: str, **kwargs
    ) -> BigqueryArchiveViewEntity:
        d = base_metadata.model_dump()
        d["defining_query"] = defining_query
        extra_fields = {k: v for k, v in kwargs.items() if k in BigqueryArchiveViewEntity.model_fields}
        extra_fields.update({"bigquery_metadata": BigqueryViewMetadata.from_dict(d)})
        return BigqueryArchiveViewEntity(**extra_fields)

    def generate_bigquery_archived_materialized_view(
        self, bigquery_item: google.cloud.bigquery.table.TableListItem, base_metadata: BigqueryBaseMetadata, defining_query: str, **kwargs
    ) -> BigqueryArchiveViewEntity:
        d = base_metadata.model_dump()
        d["defining_query"] = defining_query
        extra_fields = {k: v for k, v in kwargs.items() if k in BigqueryArchiveViewEntity.model_fields}
        extra_fields.update({"bigquery_metadata": BigqueryViewMetadata.from_dict(d)})
        return BigqueryArchiveMaterializedViewEntity(**extra_fields)

    def generate_bigquery_archived_function(
        self, bigquery_item: google.cloud.bigquery.routine.Routine, base_metadata: BigqueryBaseMetadata, **kwargs
    ) -> BigqueryArchiveFunctionEntity:
        d = base_metadata.model_dump()
        extra_fields = {k: v for k, v in kwargs.items() if k in BigqueryArchiveFunctionEntity.model_fields}
        extra_fields.update({"bigquery_metadata": BigqueryBaseMetadata.from_dict(d)})
        return BigqueryArchiveFunctionEntity(**extra_fields)

    def generate_bigquery_archived_stored_procedure(
        self, bigquery_item: google.cloud.bigquery.table.TableListItem, base_metadata: BigqueryBaseMetadata, **kwargs
    ) -> BigqueryArchiveViewEntity:
        d = base_metadata.model_dump()
        extra_fields = {k: v for k, v in kwargs.items() if k in BigqueryArchiveFunctionEntity.model_fields}
        extra_fields.update({"bigquery_metadata": BigqueryBaseMetadata.from_dict(d)})
        return BigqueryArchiveStoredProcedureEntity(**extra_fields)

    def generate_bigquery_archived_entity_from_table_item(
        self, bigquery_item: google.cloud.bigquery.table.TableListItem | google.cloud.bigquery.routine.Routine
    ) -> (
        BigqueryArchiveTableEntity
        | BigqueryArchiveViewEntity
        | BigqueryArchiveFunctionEntity
        | BigqueryArchiveMaterializedViewEntity
        | BigqueryArchiveStoredProcedureEntity
        | None
    ):
        metadata_obj = BigqueryBaseMetadata.from_dict(self.bigquery_metadata.model_dump())
        metadata_obj.identity = bigquery_item.table_id if hasattr(bigquery_item, "table_id") else bigquery_item.routine_id
        metadata_obj.labels = bigquery_item.labels if hasattr(bigquery_item, "table_id") else {}
        if type(bigquery_item) is google.cloud.bigquery.routine.Routine:
            if bigquery_item.type_ == "SCALAR_FUNCTION":
                return self.generate_bigquery_archived_function(
                    bigquery_item,
                    metadata_obj,
                    archived_datetime=self.archived_datetime,
                    gcs_prefix=self.generate_sub_serialization_prefix("function"),
                    destination_gcp_project_id=self.destination_gcp_project_id,
                    destination_bigquery_dataset=self.destination_bigquery_dataset,
                )
            elif bigquery_item.type_ == "PROCEDURE":
                return self.generate_bigquery_archived_stored_procedure(
                    bigquery_item,
                    metadata_obj,
                    archived_datetime=self.archived_datetime,
                    gcs_prefix=self.generate_sub_serialization_prefix("stored_procedure"),
                    destination_gcp_project_id=self.destination_gcp_project_id,
                    destination_bigquery_dataset=self.destination_bigquery_dataset,
                )
        else:
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
            elif bigquery_item.table_type == "MATERIALIZED_VIEW":
                return self.generate_bigquery_archived_materialized_view(
                    bigquery_item,
                    metadata_obj,
                    defining_query="Not populated yet",
                    archived_datetime=self.archived_datetime,
                    gcs_prefix=self.generate_sub_serialization_prefix("materialized_view"),
                    destination_gcp_project_id=self.destination_gcp_project_id,
                    destination_bigquery_dataset=self.destination_bigquery_dataset,
                )
            elif bigquery_item.table_type == "STORED_PROCEDURE":
                return self.generate_bigquery_archived_stored_procedure(
                    bigquery_item,
                    metadata_obj,
                    defining_query="Not populated yet",
                    archived_datetime=self.archived_datetime,
                    gcs_prefix=self.generate_sub_serialization_prefix("stored_procedure"),
                    destination_gcp_project_id=self.destination_gcp_project_id,
                    destination_bigquery_dataset=self.destination_bigquery_dataset,
                )
        return None

    def generate_sub_serialization_prefix(self, sub_type: str) -> str:
        sub_types_map = {
            "table": "tables",
            "view": "views",
            "materialized_view": "materialized_views",
            "function": "functions",
            "stored_procedure": "stored_procedures",
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

    def load_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> Self:
        with fsspec.open(self.metadata_serialized_path, "r") as f:
            loaded_model = self.model_validate(json.load(f))
            for k in loaded_model.model_fields:
                if k in BigqueryArchivedDatasetEntity.model_fields:
                    setattr(self, k, getattr(loaded_model, k))

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        dataset = bigquery_client.get_dataset(self.fully_qualified_identity)
        self.bigquery_metadata.description = dataset.description
        self.bigquery_metadata.labels = dataset.labels

    def archive_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, archive_config: dict = None) -> typing.Any:
        self.is_archived = True
        with fsspec.open(self.metadata_serialized_path, "w") as f:
            f.write(self.model_dump_json(indent=2))

    def restore_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, restore_config: dict = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        if not restore_config:
            restore_config = {}
        fully_qualified_identity = self.fully_qualified_identity
        if self.destination_gcp_project_id and self.destination_bigquery_dataset:
            fully_qualified_identity = f"{self.destination_gcp_project_id}.{self.destination_bigquery_dataset}"
        if restore_config.get("overwrite_existing", False):
            bigquery_client.delete_dataset(fully_qualified_identity, delete_contents=True, not_found_ok=True)
        dataset = bigquery_client.create_dataset(fully_qualified_identity, exists_ok=True)
        dataset.description = self.bigquery_metadata.description
        dataset.labels = self.bigquery_metadata.labels
        bigquery_client.update_dataset(dataset, ["description", "labels"])
