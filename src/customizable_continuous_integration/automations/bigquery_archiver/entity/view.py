"""This module defines archived view entity classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  23/02/2025   Ryan, Gao       Initial creation
  05/03/2025   Ryan, Gao       Use sqlparse to handle view query transformation
  10/04/2025   Ryan, Gao       Add archive timestamp to dataset labels; Add skip_restore
  15/06/2025   Ryan, Gao       Fix restore logic to replace UDF in view query
"""

import json
import typing

import fsspec
import google.cloud.bigquery.table
import sqlglot
from typing_extensions import Self

from customizable_continuous_integration.automations.bigquery_archiver.entity.base import BigqueryBaseArchiveEntity, BigquerySchemaFieldEntity
from customizable_continuous_integration.automations.bigquery_archiver.entity.bigquery_metadata import BigqueryPartitionConfig, BigqueryViewMetadata
from customizable_continuous_integration.common_libs.sql.parsing.extract_dependencies import extract_sql_select_statement_dependencies


class BigqueryArchiveViewEntity(BigqueryBaseArchiveEntity):
    bigquery_metadata: BigqueryViewMetadata
    defining_query: str = ""
    schema_fields: list[BigquerySchemaFieldEntity] = []

    @property
    def entity_type(self) -> str:
        return "view"

    @property
    def metadata_serialized_path(self):
        return f"{self.gcs_prefix}/view={self.identity}/view.json"

    @property
    def dependencies(self) -> set[str]:
        ret = set()
        for e in extract_sql_select_statement_dependencies(self.defining_query, set()):
            if len(e.split(".")) == 3:
                ret.add(e)
            elif len(e.split(".")) == 2:
                ret.add(f"{self.project_id}.{e}")
            else:
                ret.add(f"{self.project_id}.{self.dataset}.{e}")
        return ret

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
        if restore_config.get("skip_restore", {}).get(self.identity, False):
            print(f"Skip restoring {self.entity_type} {fully_qualified_identity}")
            return
        if restore_config.get("overwrite_existing", False):
            bigquery_client.delete_table(fully_qualified_identity, not_found_ok=True)
        view = google.cloud.bigquery.Table(fully_qualified_identity)
        view.view_query = self.defining_query
        view = bigquery_client.create_table(view, exists_ok=True)
        view.description = self.bigquery_metadata.description
        view.labels = self.bigquery_metadata.labels
        if restore_config.get("attach_archive_ts_to_label", True):
            view.labels["archive_ts"] = self.archived_datetime_str
        view.schema = [f.to_bigquery_schema_field() for f in self.schema_fields] if self.schema_fields else None
        table = bigquery_client.update_table(view, ["description", "schema", "labels"])
        return table

    def modify_self_query(self, modify_config: dict) -> typing.Any:
        replacement_mapping = modify_config.get("replacement_mapping", {})
        parsed_query = sqlglot.parse_one(self.defining_query, dialect="bigquery")
        for k, v in replacement_mapping.items():
            if len(k.split(".")) == 2:
                src_catalog, src_db = k.split(".")
                dst_catalog, dst_db = v.split(".")
            elif len(k.split(".")) == 1:
                src_catalog, src_db = (None, k)
                dst_catalog, dst_db = (None, v)
            else:
                continue
            for te in parsed_query.find_all(sqlglot.exp.Table):
                search_full_id = f"{src_catalog + '.' if src_catalog else ''}{src_db + '.' if src_db else ''}{te.name}"
                te_full_id = f"{te.catalog + '.' if te.catalog else ''}{te.db + '.' if te.db else ''}{te.name}"
                if te_full_id == search_full_id:
                    te.replace(sqlglot.table(te.name, catalog=dst_catalog, db=dst_db, alias=te.alias))
            for te in parsed_query.find_all(sqlglot.exp.Anonymous):
                search_full_ref = f"{src_catalog + '.' if src_catalog else ''}{src_db if src_db else ''}"
                te_full_ref = te.parent.this.this if type(te.parent.this) is not str else ""
                if te_full_ref == search_full_ref:
                    te.parent.this.set(arg_key="this", value=f"{dst_catalog + '.' if dst_catalog else ''}{dst_db + '.' if dst_db else ''}")
        self.defining_query = parsed_query.sql(dialect="bigquery", pretty=True)


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
        return f"{self.gcs_prefix}/view={self.identity}/materialized_view.json"

    @property
    def dependencies(self) -> set[str]:
        ret = set()
        project_id = self.destination_gcp_project_id or self.project_id
        dataset = self.destination_bigquery_dataset or self.dataset
        for e in extract_sql_select_statement_dependencies(self.mview_query, set()):
            if len(e.split(".")) == 3:
                ret.add(e)
            elif len(e.split(".")) == 2:
                ret.add(f"{project_id}.{e}")
            else:
                ret.add(f"{project_id}.{dataset}.{e}")
        return ret

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
        if restore_config.get("skip_restore", {}).get(self.identity, False):
            print(f"Skip restoring {self.entity_type} {fully_qualified_identity}")
            return
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
        if restore_config.get("attach_archive_ts_to_label", True):
            view.labels["archive_ts"] = self.archived_datetime_str
        view.schema = [f.to_bigquery_schema_field() for f in self.schema_fields] if self.schema_fields else None
        table = bigquery_client.update_table(view, ["description", "labels"])
        return table

    def modify_self_query(self, modify_config: dict) -> typing.Any:
        replacement_mapping = modify_config.get("replacement_mapping", {})
        for k, v in replacement_mapping.items():
            if len(k.split(".")) == 3:
                src_catalog, src_db, src_name = k.split(".")
                dst_catalog, dst_db, dst_name = v.split(".")
            elif len(k.split(".")) == 2:
                src_catalog, src_db, src_name = (None, *k.split("."))
                dst_catalog, dst_db, dst_name = (None, *v.split("."))
            else:
                src_catalog, src_db, src_name = (None, None, k)
                dst_catalog, dst_db, dst_name = (None, None, v)
            search_full_id = f"{src_catalog + '.' if src_catalog else ''}{src_db + '.' if src_db else ''}{src_name}"
            parsed_query = sqlglot.parse_one(self.mview_query, dialect="bigquery")
            for te in parsed_query.find_all(sqlglot.exp.Table):
                te_full_id = f"{te.catalog + '.' if te.catalog else ''}{te.db + '.' if te.db else ''}{te.name}"
                if te_full_id == search_full_id:
                    te.replace(sqlglot.table(dst_name, catalog=dst_catalog, db=dst_db, alias=te.alias))
            self.mview_query = parsed_query.sql(dialect="bigquery", pretty=True)
