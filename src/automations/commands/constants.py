"""This module defines constants for CLI commands

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  03/09/2024   Ryan, Gao       Initial creation
"""

import typing

from automations.commands.integration_test import integration_command
from automations.commands.write_protection_hook import write_protection_command
from common_libs.collections import ImmutableDictWrapper

CLICommandHandlerType = typing.Callable[[list[str]], None]


def not_implemented_command(cli_args: list[str]) -> None:
    raise NotImplementedError("This command is not implemented.")


SentinelCommand = not_implemented_command

INTEGRATION_CLI_COMMANDS_REGISTRY: ImmutableDictWrapper[str, CLICommandHandlerType] = ImmutableDictWrapper(
    {"integration-test": integration_command, "write-protection": write_protection_command}
)


def retrieve_cli_command(command_name: str) -> CLICommandHandlerType:
    if command_name in INTEGRATION_CLI_COMMANDS_REGISTRY:
        return INTEGRATION_CLI_COMMANDS_REGISTRY[command_name]
    return SentinelCommand