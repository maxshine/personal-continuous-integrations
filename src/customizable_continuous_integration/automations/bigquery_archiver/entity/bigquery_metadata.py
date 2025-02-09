"""This module defines bigquery native metadata classes

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  08/02/2025   Ryan, Gao       Initial creation
"""

import pydantic
from typing_extensions import Self


class BigqueryPartitionConfig(pydantic.BaseModel):
    partition_type: str
    partition_field: str
    partition_expiration_ms: int
    partition_require_filter: bool


class BigqueryBaseMetadata(pydantic.BaseModel):
    project_id: str
    dataset: str
    identity: str
    description: str = ""
    labels: dict[str, str] = {}
    tags: dict[str, str] = {}

    @classmethod
    def from_dict(cls, metadata_dict: dict) -> Self:
        d = {k: v for k, v in metadata_dict.items() if k in BigqueryBaseMetadata.model_fields}
        return BigqueryBaseMetadata(**d)


class BigqueryDatasetMetadata(BigqueryBaseMetadata): ...


class BigqueryTableMetadata(BigqueryBaseMetadata):
    partition_config: BigqueryPartitionConfig | None = None

    @classmethod
    def from_dict(cls, metadata_dict: dict) -> Self:
        d = {k: v for k, v in metadata_dict.items() if k in BigqueryTableMetadata.model_fields}
        return BigqueryTableMetadata(**d)


class BigqueryViewMetadata(BigqueryBaseMetadata):
    defining_query: str

    @classmethod
    def from_dict(cls, metadata_dict: dict) -> Self:
        d = {k: v for k, v in metadata_dict.items() if k in BigqueryViewMetadata.model_fields}
        return BigqueryViewMetadata(**d)
