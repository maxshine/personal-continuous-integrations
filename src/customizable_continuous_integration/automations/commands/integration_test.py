"""The entrypoint to the integration module

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  03/09/2024   Ryan, Gao       Initial creation
  31/10/2024   Ryan, Gao       Refactor the input parameter automation-config-file
  21/06/2025   Ryan, Gao       Add variadic parameters
"""

import pathlib

import yaml

from customizable_continuous_integration.automations.integration.argument import generate_arguments_parser
from customizable_continuous_integration.automations.integration.executor import execute_commands_in_serial, prepare_test_environment
from customizable_continuous_integration.automations.integration.logging import _logger


def integration_command(cli_args: list[str], *args, **kargs) -> None:
    args_parser = generate_arguments_parser()
    args = args_parser.parse_args(cli_args)
    if args.automation_config_file:
        config_file_path = pathlib.Path(args.automation_config_file).resolve()
        with open(config_file_path, "r") as f:
            integration_test_config = yaml.safe_load(f)
        _logger.info(f"Integration test config:\n {integration_test_config}")
        execute_commands_in_serial(integration_test_config)
    else:
        _logger.info("No test config file specified")
    exit(0)
