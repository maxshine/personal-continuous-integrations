"""The entrypoint to the integration module

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  03/09/2024   Ryan, Gao       Initial creation
"""

import pathlib

import yaml

from automations.integration.argument import generate_arguments_parser
from automations.integration.executor import execute_commands_in_serial, prepare_test_environment
from automations.integration.logging import _logger


def integration_command(cli_args: list[str]) -> None:
    args_parser = generate_arguments_parser()
    args = args_parser.parse_args(cli_args)
    config_file_path = pathlib.Path(args.config_file).resolve()
    with open(config_file_path, "r") as f:
        integration_test_config = yaml.safe_load(f)
    _logger.info(f"Integration test config:\n {integration_test_config}")
    execute_commands_in_serial(integration_test_config)
    exit(0)
