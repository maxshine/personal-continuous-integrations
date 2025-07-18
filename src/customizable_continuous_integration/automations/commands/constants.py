"""This module defines constants for CLI commands

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  03/09/2024   Ryan, Gao       Initial creation
  08/02/2025   Ryan, Gao       Add archive and restore bigquery commands
  06/03/2025   Ryan, Gao       Add help command to show info
  28/03/2025   Ryan, Gao       Add default help command
  21/06/2025   Ryan, Gao       Add variadic parameters to commands dictionary
"""

import typing

from customizable_continuous_integration.automations.commands.archive_bigquery import archive_command, restore_command
from customizable_continuous_integration.automations.commands.integration_test import integration_command
from customizable_continuous_integration.automations.commands.run_shell import run_shell_commands
from customizable_continuous_integration.automations.commands.write_protection_hook import write_protection_command
from customizable_continuous_integration.common_libs.collections import ImmutableDictWrapper

CLICommandHandlerType = typing.Callable[[list[str], list, dict], None]


def not_implemented_command(cli_args: list[str], *args, **kargs) -> None:
    raise NotImplementedError("This command is not implemented.")


SentinelCommand = not_implemented_command

INTEGRATION_CLI_COMMANDS_REGISTRY: ImmutableDictWrapper[str, CLICommandHandlerType] = ImmutableDictWrapper(
    {
        "integration-test": integration_command,
        "run-shell": run_shell_commands,
        "write-protection": write_protection_command,
        "archive-bigquery": archive_command,
        "restore-bigquery": restore_command,
    }
)


def retrieve_cli_command(command_name: str) -> CLICommandHandlerType:
    if command_name in INTEGRATION_CLI_COMMANDS_REGISTRY:
        return INTEGRATION_CLI_COMMANDS_REGISTRY[command_name]
    print("Available commands:")
    print("\n".join(INTEGRATION_CLI_COMMANDS_REGISTRY.keys()))
    exit(0)
