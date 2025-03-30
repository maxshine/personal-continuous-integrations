"""The entrypoint to the Bigquery Archiver

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  08/02/2025   Ryan, Gao       Initial creation
  28/03/2025   Ryan, Gao       Add default help argument
"""

import argparse
import json
import logging
import sys

import fsspec
import yaml

from customizable_continuous_integration.automations.bigquery_archiver.executor.archive import ArchiveSourceBigqueryDatasetExecutor
from customizable_continuous_integration.automations.bigquery_archiver.executor.fetch import FetchSourceBigqueryDatasetExecutor
from customizable_continuous_integration.automations.bigquery_archiver.executor.restore import RestoreBigqueryDatasetExecutor


def get_bigquery_archiver_logger(logger_name: str) -> logging.Logger:
    _logger = logging.getLogger(logger_name)
    logging_ch = logging.StreamHandler(sys.stdout)
    logging_ch.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(lineno)s: %(message)s"))
    _logger.addHandler(logging_ch)
    _logger.setLevel(logging.INFO)
    return _logger


def generate_archive_arguments_parser() -> argparse.ArgumentParser:
    args_parser = argparse.ArgumentParser(add_help=True)
    args_parser.add_argument("--archive-config-file", default="")
    args_parser.add_argument("--archive-source-gcp-project-id", default="")
    args_parser.add_argument("--archive-source-bigquery-dataset", default="")
    args_parser.add_argument("--archive-destination-gcs-prefix", default="")
    return args_parser


def generate_restore_arguments_parser() -> argparse.ArgumentParser:
    args_parser = argparse.ArgumentParser(add_help=True)
    args_parser.add_argument("--restore-config-file", default="")
    args_parser.add_argument("--restore-destination-gcp-project-id", default="")
    args_parser.add_argument("--restore-destination-bigquery-dataset", default="")
    args_parser.add_argument("--restore-source-gcs-archive", default="")
    return args_parser


def archive_command(cli_args: list[str]) -> None:
    _logger = get_bigquery_archiver_logger("bigquery_archive")
    args_parser = generate_archive_arguments_parser()
    args = args_parser.parse_args(cli_args)
    archive_configs = [{"task_type": "archive"}]
    if args.archive_config_file:
        with fsspec.open(args.archive_config_file) as f:
            archive_configs = yaml.safe_load(f)
    for archive_config in archive_configs:
        if archive_config.get("task_type", "") != "archive":
            continue
        if args.archive_source_gcp_project_id:
            archive_config["source_gcp_project_id"] = args.archive_source_gcp_project_id
        if args.archive_source_bigquery_dataset:
            archive_config["source_bigquery_dataset"] = args.archive_source_bigquery_dataset
        if args.archive_destination_gcs_prefix:
            archive_config["destination_gcs_prefix"] = args.archive_destination_gcs_prefix
        _logger.info(f"Archiving task {archive_config.get('name', 'ad-hoc')} with config: {archive_config}")
        bigquery_dataset_config = {
            "project_id": archive_config["source_gcp_project_id"],
            "dataset": archive_config["source_bigquery_dataset"],
            "identity": archive_config["source_bigquery_dataset"],
            "gcs_prefix": archive_config["destination_gcs_prefix"],
        }
        dataset_entity = FetchSourceBigqueryDatasetExecutor(bigquery_archived_dataset_config=bigquery_dataset_config, logger=_logger).execute()
        archive_executor = ArchiveSourceBigqueryDatasetExecutor(
            bigquery_archived_dataset_entity=dataset_entity, archive_config=archive_config, logger=_logger
        )
        dataset_entity = archive_executor.execute()
        _logger.info(f"Archived dataset :\n {dataset_entity.model_dump_json(indent=2)}")
        _logger.info(f"Archived dataset is located: {dataset_entity.metadata_serialized_path.rstrip('/dataset.json')}")
        _logger.info(f"Archiving task {archive_config.get('name', 'ad-hoc')} completed")
    exit(0)


def restore_command(cli_args: list[str]) -> None:
    _logger = get_bigquery_archiver_logger("bigquery_restore")
    args_parser = generate_restore_arguments_parser()
    args = args_parser.parse_args(cli_args)
    restore_configs = [{"task_type": "restore"}]
    if args.restore_config_file:
        with fsspec.open(args.restore_config_file) as f:
            restore_configs = yaml.safe_load(f)
    for restore_config in restore_configs:
        if restore_config.get("task_type", "") != "restore":
            continue
        if args.restore_destination_gcp_project_id:
            restore_config["destination_gcp_project_id"] = args.restore_destination_gcp_project_id
        if args.restore_destination_bigquery_dataset:
            restore_config["destination_bigquery_dataset"] = args.restore_destination_bigquery_dataset
        if args.restore_source_gcs_archive:
            restore_config["source_gcs_archive"] = args.restore_source_gcs_archive
        _logger.info(f"Restoring task {restore_config.get('name', 'ad-hoc')} with config: {restore_config}")
        with fsspec.open(f"{restore_config['source_gcs_archive']}/dataset.json") as f:
            bigquery_dataset_config = json.load(f)
        bigquery_dataset_config["destination_gcp_project_id"] = restore_config["destination_gcp_project_id"]
        bigquery_dataset_config["destination_bigquery_dataset"] = restore_config["destination_bigquery_dataset"]
        restore_executor = RestoreBigqueryDatasetExecutor(
            bigquery_archived_dataset_config=bigquery_dataset_config, restore_config={"overwrite_existing": True}, logger=_logger
        )
        restore_executor.execute()
        _logger.info(f"Restoring task {restore_config.get('name', 'ad-hoc')} completed")
    exit(0)
