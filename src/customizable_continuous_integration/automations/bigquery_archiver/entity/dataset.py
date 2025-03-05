"""This module defines archived dataset entity classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  23/02/2025   Ryan, Gao       Initial creation
"""

import datetime
import json
import typing

import fsspec
import google.cloud.bigquery.table
from typing_extensions import Self

from customizable_continuous_integration.automations.bigquery_archiver.entity.base import BigqueryBaseArchiveEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.bigquery_metadata import (
    BigqueryBaseMetadata,
    BigqueryDatasetMetadata,
    BigqueryPartitionConfig,
    BigqueryTableMetadata,
    BigqueryViewMetadata,
)
from customizable_continuous_integration.automations.bigquery_archiver.entity.routine import (
    BigqueryArchiveFunctionEntity,
    BigqueryArchiveStoredProcedureEntity,
)
from customizable_continuous_integration.automations.bigquery_archiver.entity.table import BigqueryArchiveTableEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.view import (
    BigqueryArchiveMaterializedViewEntity,
    BigqueryArchiveViewEntity,
)


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
        extra_fields = {k: v for k, v in kwargs.items() if k in BigqueryArchiveTableEntity.model_fields}
        extra_fields.update({"bigquery_metadata": BigqueryTableMetadata.from_dict(d), "partition_config": partition_config})
        return BigqueryArchiveTableEntity(**extra_fields)

    def generate_bigquery_archived_view(
        self, bigquery_item: google.cloud.bigquery.table.TableListItem, base_metadata: BigqueryBaseMetadata, defining_query: str, **kwargs
    ) -> BigqueryArchiveViewEntity:
        d = base_metadata.model_dump()
        extra_fields = {k: v for k, v in kwargs.items() if k in BigqueryArchiveViewEntity.model_fields}
        extra_fields.update({"bigquery_metadata": BigqueryViewMetadata.from_dict(d), "defining_query": defining_query})
        return BigqueryArchiveViewEntity(**extra_fields)

    def generate_bigquery_archived_materialized_view(
        self, bigquery_item: google.cloud.bigquery.table.TableListItem, base_metadata: BigqueryBaseMetadata, defining_query: str, **kwargs
    ) -> BigqueryArchiveMaterializedViewEntity:
        d = base_metadata.model_dump()
        extra_fields = {k: v for k, v in kwargs.items() if k in BigqueryArchiveViewEntity.model_fields}
        extra_fields.update({"bigquery_metadata": BigqueryViewMetadata.from_dict(d), "mview_query": defining_query})
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
                    metadata_schema=self.metadata_version,
                )
            elif bigquery_item.type_ == "PROCEDURE":
                return self.generate_bigquery_archived_stored_procedure(
                    bigquery_item,
                    metadata_obj,
                    archived_datetime=self.archived_datetime,
                    gcs_prefix=self.generate_sub_serialization_prefix("stored_procedure"),
                    destination_gcp_project_id=self.destination_gcp_project_id,
                    destination_bigquery_dataset=self.destination_bigquery_dataset,
                    metadata_schema=self.metadata_version,
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
                    metadata_schema=self.metadata_version,
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
                    metadata_schema=self.metadata_version,
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
                    metadata_schema=self.metadata_version,
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
                    metadata_schema=self.metadata_version,
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

    def modify_sub_entity_queries(self, **kwargs):
        if (self.destination_gcp_project_id and self.destination_gcp_project_id != self.project_id) or (
            self.destination_bigquery_dataset and self.destination_bigquery_dataset != self.dataset
        ):
            replacement_mapping = {}
            replacement_mapping[f"{self.project_id}.{self.dataset}"] = f"{self.destination_gcp_project_id}.{self.destination_bigquery_dataset}"
            replacement_mapping[f"{self.dataset}"] = f"{self.destination_bigquery_dataset}"
            modify_config = {"replacement_mapping": replacement_mapping}
            for t in self.views:
                t.modify_self_query(modify_config)
            for t in self.materialized_views:
                t.modify_self_query(modify_config)
            for t in self.user_define_functions:
                t.modify_self_query(modify_config)
            for t in self.stored_procedures:
                t.modify_self_query(modify_config)

    def populate_sub_restore_info(self) -> typing.Any:
        for t in self.tables:
            t.destination_gcp_project_id = self.destination_gcp_project_id
            t.destination_bigquery_dataset = self.destination_bigquery_dataset
        for t in self.views:
            t.destination_gcp_project_id = self.destination_gcp_project_id
            t.destination_bigquery_dataset = self.destination_bigquery_dataset
        for t in self.materialized_views:
            t.destination_gcp_project_id = self.destination_gcp_project_id
            t.destination_bigquery_dataset = self.destination_bigquery_dataset
        for t in self.user_define_functions:
            t.destination_gcp_project_id = self.destination_gcp_project_id
            t.destination_bigquery_dataset = self.destination_bigquery_dataset
        for t in self.stored_procedures:
            t.destination_gcp_project_id = self.destination_gcp_project_id
            t.destination_bigquery_dataset = self.destination_bigquery_dataset
        self.modify_sub_entity_queries()
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
