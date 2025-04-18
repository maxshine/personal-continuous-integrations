"""This module defines bigquery native metadata classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  08/02/2025   Ryan, Gao       Initial creation
  11/04/2025   Ryan, Gao       Add range partitioning in BigqueryPartitionConfig
"""

import google.cloud.bigquery
import pydantic
from typing_extensions import Self


class BigqueryPartitionConfig(pydantic.BaseModel):
    partition_type: str
    partition_field: str
    partition_expiration_ms: int
    partition_require_filter: bool
    partition_category: str = "TIME"
    partition_range: list[int] = []

    def to_bigquery_time_partitioning(self) -> google.cloud.bigquery.table.TimePartitioning:
        return google.cloud.bigquery.table.TimePartitioning(
            type_=self.partition_type,
            field=self.partition_field,
            expiration_ms=self.partition_expiration_ms if self.partition_expiration_ms > 0 else None,
            require_partition_filter=self.partition_require_filter,
        )

    def to_bigquery_range_partitioning(self) -> google.cloud.bigquery.table.RangePartitioning:
        return google.cloud.bigquery.table.RangePartitioning(
            field=self.partition_field,
            range_=google.cloud.bigquery.table.PartitionRange(
                start=self.partition_range[0], end=self.partition_range[1], interval=self.partition_range[2]
            ),
        )


class BigqueryBaseMetadata(pydantic.BaseModel):
    project_id: str
    dataset: str
    identity: str
    description: str | None = ""
    labels: dict[str, str] = {}
    tags: dict[str, str] = {}

    @classmethod
    def from_dict(cls, metadata_dict: dict) -> Self:
        d = {k: v for k, v in metadata_dict.items() if k in BigqueryBaseMetadata.model_fields}
        return BigqueryBaseMetadata(**d)


class BigqueryDatasetMetadata(BigqueryBaseMetadata): ...


class BigqueryTableMetadata(BigqueryBaseMetadata):
    @classmethod
    def from_dict(cls, metadata_dict: dict) -> Self:
        d = {k: v for k, v in metadata_dict.items() if k in BigqueryTableMetadata.model_fields}
        return BigqueryTableMetadata(**d)


class BigqueryViewMetadata(BigqueryBaseMetadata):
    @classmethod
    def from_dict(cls, metadata_dict: dict) -> Self:
        d = {k: v for k, v in metadata_dict.items() if k in BigqueryViewMetadata.model_fields}
        return BigqueryViewMetadata(**d)
