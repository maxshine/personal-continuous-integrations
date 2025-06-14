"""This module defines archived routine entity classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  23/02/2025   Ryan, Gao       Initial creation
  10/04/2025   Ryan, Gao       Add description field in the restore method; Add skip_restore
  12/06/2025   Ryan, Gao       Add js function with STRUCT return type support
"""

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
        return f"{self.gcs_prefix}/function={self.identity}/function.json"

    def fetch_self(self, bigquery_client: google.cloud.bigquery.client.Client = None) -> typing.Any:
        if not bigquery_client:
            bigquery_client = google.cloud.bigquery.Client(project=self.project_id)
        routine = bigquery_client.get_routine(self.fully_qualified_identity)
        self.body = routine.body
        self.imported_libraries = routine.imported_libraries
        self.arguments = [{"name": arg.name, "data_type": arg.data_type.type_kind.value} for arg in routine.arguments]
        self.return_type = routine.return_type.type_kind.value
        if self.return_type == "STRUCT":
            self.return_type = (
                f"STRUCT<{', '.join([f'{field.name} {field.type.type_kind.value}' for field in routine.return_type.struct_type.fields])}>"
            )
        self.language = routine.language
        self.bigquery_metadata.description = routine.description

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
        if restore_config.get("skip_restore", {}).get(self.identity, False):
            print(f"Skip restoring {self.entity_type} {fully_qualified_identity}")
            return
        if restore_config.get("overwrite_existing", False):
            bigquery_client.delete_routine(fully_qualified_identity, not_found_ok=True)
        language_clause = {"JAVASCRIPT": "LANGUAGE js", "PYTHON": "LANGUAGE python"}.get(self.language, "")
        left_syntax_delimiter = "r\"\"\"" if self.language != "SQL" else "("
        right_syntax_delimiter = "\"\"\"" if self.language != "SQL" else ")"
        stmt = f"""CREATE FUNCTION `{fully_qualified_identity}`({", ".join([f"{arg['name']} {arg['data_type']}" for arg in self.arguments])})
                   RETURNS {self.return_type}
                   {language_clause}
                   AS {left_syntax_delimiter}{self.body}{right_syntax_delimiter}
                """
        job = bigquery_client.query(stmt)
        job.result()
        routine = bigquery_client.get_routine(fully_qualified_identity)
        routine.description = self.bigquery_metadata.description
        routine = bigquery_client.update_routine(routine, ["description", "type_", "body", "arguments", "language", "return_type"])
        return routine

    def modify_self_query(self, modify_config: dict) -> typing.Any:
        replacement_mapping = modify_config.get("replacement_mapping", {})
        for k, v in replacement_mapping.items():
            self.body = self.body.replace(k, v)


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
        return f"{self.gcs_prefix}/stored_procedure={self.identity}/stored_procedure.json"

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
            bigquery_client.delete_routine(fully_qualified_identity, not_found_ok=True)
        stmt = f"""CREATE PROCEDURE `{fully_qualified_identity}`({", ".join([f"{arg['name']} {arg['data_type']}" for arg in self.arguments])})
                   {self.body}
                """
        job = bigquery_client.query(stmt)
        job.result()
        routine = bigquery_client.get_routine(fully_qualified_identity)
        routine.description = self.bigquery_metadata.description
        routine = bigquery_client.update_routine(routine, ["description", "type_", "body", "arguments", "language", "return_type"])
        return routine

    def modify_self_query(self, modify_config: dict) -> typing.Any:
        replacement_mapping = modify_config.get("replacement_mapping", {})
        for k, v in replacement_mapping.items():
            self.body = self.body.replace(k, v)
