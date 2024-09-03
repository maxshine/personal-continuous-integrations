"""This module defines constants

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
"""

import typing

from customizable_continuous_integration.automations.integration.test_commands.base.base_command import BaseAutomationCommand
from customizable_continuous_integration.automations.integration.test_commands.dbt_test import DBTAutomationTestCommand
from customizable_continuous_integration.common_libs.collections import ImmutableDictWrapper

SentinelCommand = BaseAutomationCommand

INTEGRATION_TEST_COMMANDS_REGISTRY: ImmutableDictWrapper[str, type[BaseAutomationCommand]] = ImmutableDictWrapper(
    {"dbt_test": DBTAutomationTestCommand}
)


def retrieve_test_command(test_name: str) -> typing.Any:
    if test_name in INTEGRATION_TEST_COMMANDS_REGISTRY:
        return INTEGRATION_TEST_COMMANDS_REGISTRY[test_name]
    return SentinelCommand
