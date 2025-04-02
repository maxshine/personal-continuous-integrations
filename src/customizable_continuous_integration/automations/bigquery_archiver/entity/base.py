"""This module defines base archived entity classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  23/02/2025   Ryan, Gao       Initial creation
  03/03/2025   Ryan, Gao       Add dependencies property
  23/03/2025   Ryan, Gao       Add DAGNodeInterface
  02/04/2025   Ryan, Gao       Add archiver version for backwards compatibility
"""

import datetime
import typing

import google.cloud.bigquery.table
import pydantic
from typing_extensions import Self

from customizable_continuous_integration.automations.bigquery_archiver.entity.bigquery_metadata import BigqueryBaseMetadata
from customizable_continuous_integration.common_libs.graph.dag.entity import DAGNodeInterface


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

    def to_bigquery_schema_field(self) -> google.cloud.bigquery.SchemaField:
        schema_field = google.cloud.bigquery.SchemaField(
            self.name,
            self.type,
            mode=self.mode,
            description=self.description,
            default_value_expression=self.default_value_expression,
            fields=[f.to_bigquery_schema_field() for f in self.fields] if self.fields else [],
        )
        return schema_field


class BigqueryBaseArchiveEntity(pydantic.BaseModel, DAGNodeInterface):
    bigquery_metadata: BigqueryBaseMetadata
    metadata_version: str = "v1"
    archiver_version: str = "v1"
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
        return (
            f"{self.project_id}.{self.dataset}.{self.identity}"
            if not self.destination_gcp_project_id
            else f"{self.destination_gcp_project_id}.{self.destination_bigquery_dataset}.{self.identity}"
        )

    @property
    def dependencies(self) -> set[str]:
        return set()

    def dag_dependencies(self) -> set[str]:
        return self.dependencies

    def dag_key(self) -> str:
        return self.fully_qualified_identity

    def from_dataset_reference(self, dataset_reference: str):
        pass

    def modify_self_query(self, modify_config: dict) -> typing.Any:
        raise NotImplementedError("Please implement me to modify myself")

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client) -> typing.Any:
        raise NotImplementedError("Please implement me to fetch myself")

    def archive_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, archive_config: dict = None) -> typing.Any:
        raise NotImplementedError("Please implement me to fetch myself")

    def load_self(self, bigquery_client: google.cloud.bigquery.client.Client) -> typing.Any:
        raise NotImplementedError("Please implement me to fetch myself")

    def restore_self(self, bigquery_client: google.cloud.bigquery.client.Client = None, _config: dict = None) -> typing.Any:
        raise NotImplementedError("Please implement me to fetch myself")
