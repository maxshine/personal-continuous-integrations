"""This module defines constants for CLI commands

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  03/09/2024   Ryan, Gao       Initial creation
"""

import typing

from customizable_continuous_integration.automations.commands.integration_test import integration_command
from customizable_continuous_integration.automations.commands.run_shell import run_shell_commands
from customizable_continuous_integration.automations.commands.write_protection_hook import write_protection_command
from customizable_continuous_integration.common_libs.collections import ImmutableDictWrapper

CLICommandHandlerType = typing.Callable[[list[str]], None]


def not_implemented_command(cli_args: list[str]) -> None:
    raise NotImplementedError("This command is not implemented.")


SentinelCommand = not_implemented_command

INTEGRATION_CLI_COMMANDS_REGISTRY: ImmutableDictWrapper[str, CLICommandHandlerType] = ImmutableDictWrapper(
    {"integration-test": integration_command, "run-shell": run_shell_commands, "write-protection": write_protection_command}
)


def retrieve_cli_command(command_name: str) -> CLICommandHandlerType:
    if command_name in INTEGRATION_CLI_COMMANDS_REGISTRY:
        return INTEGRATION_CLI_COMMANDS_REGISTRY[command_name]
    return SentinelCommand
