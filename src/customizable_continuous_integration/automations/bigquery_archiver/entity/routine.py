"""This module defines archived routine entity classes

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

from customizable_continuous_integration.automations.bigquery_archiver.entity.base import BigqueryBaseArchiveEntity, BigquerySchemaFieldEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.bigquery_metadata import BigqueryBaseMetadata


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


class BigqueryArchiveStoredProcedureEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryBaseMetadata
    body: str = ""
    imported_libraries: list[str] = []
    arguments: list[dict] = []
    language: str = "SQL"
    return_type: str = ""

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
