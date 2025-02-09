"""The entrypoint to the Bigquery Archiver

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  08/02/2025   Ryan, Gao       Initial creation
"""

import argparse
import logging
import sys

from customizable_continuous_integration.automations.bigquery_archiver.entity.archive import BigqueryArchivedDatasetEntity
from customizable_continuous_integration.automations.bigquery_archiver.executor.fetch import FetchSourceBigqueryDatasetExecutor


def get_bigquery_archiver_logger(logger_name: str) -> logging.Logger:
    _logger = logging.getLogger(logger_name)
    logging_ch = logging.StreamHandler(sys.stdout)
    logging_ch.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(lineno)s: %(message)s"))
    _logger.addHandler(logging_ch)
    _logger.setLevel(logging.INFO)
    return _logger


def generate_archive_arguments_parser() -> argparse.ArgumentParser:
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("--archive-config-file", default="")
    args_parser.add_argument("--archive-source-gcp-project-id", default="")
    args_parser.add_argument("--archive-source-bigquery-dataset", default="")
    args_parser.add_argument("--archive-destination-gcs-archive", default="")
    args_parser.add_argument("--archive-destination-gcs-prefix", default="")
    return args_parser


def generate_restore_arguments_parser() -> argparse.ArgumentParser:
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("--restore-config-file", default="")
    args_parser.add_argument("--restore-destination-gcp-project-id", default="")
    args_parser.add_argument("--restore-destination-bigquery-dataset", default="")
    args_parser.add_argument("--restore-source-gcs-archive", default="")
    args_parser.add_argument("--restore-source-gcs-prefix", default="")
    return args_parser


def archive_command(cli_args: list[str]) -> None:
    _logger = get_bigquery_archiver_logger("bigquery_archive")
    args_parser = generate_archive_arguments_parser()
    args = args_parser.parse_args(cli_args)
    dataset_entity = BigqueryArchivedDatasetEntity.from_dict(
        {
            "project_id": args.archive_source_gcp_project_id,
            "dataset": args.archive_source_bigquery_dataset,
            "identity": args.archive_source_bigquery_dataset,
            "gcs_prefix": args.archive_destination_gcs_prefix,
        }
    )
    dataset_entity = FetchSourceBigqueryDatasetExecutor(bigquery_archived_dataset_entity=dataset_entity, logger=_logger).execute()
    _logger.info(f"Archived dataset :\n {dataset_entity.model_dump_json(indent=2)}")
    exit(0)


def restore_command(cli_args: list[str]) -> None:
    _logger = get_bigquery_archiver_logger("bigquery_restore")
    args_parser = generate_restore_arguments_parser()
    args = args_parser.parse_args(cli_args)
    exit(0)
