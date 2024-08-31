"""This is the entrypoint to the automation module

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
"""

import pathlib
import sys

import yaml

from automations.argument import generate_arguments_parser
from automations.executor import execute_commands_in_serial, prepare_test_environment
from automations.logging import _logger


def main() -> None:
    prepare_test_environment()
    args_parser = generate_arguments_parser()
    args = args_parser.parse_args(sys.argv[1:])
    config_file_path = pathlib.Path(args.config_file).resolve()
    with open(config_file_path, "r") as f:
        integration_test_config = yaml.safe_load(f)
    _logger.info(f"Integration test config:\n {integration_test_config}")
    execute_commands_in_serial(integration_test_config)
    exit(0)


if __name__ == "__main__":
    main()
