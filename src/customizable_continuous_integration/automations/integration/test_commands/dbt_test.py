"""This module defines DBT Test automation command.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
  01/11/2024   Ryan, Gao       Initial creation
"""

import os
import pathlib
import tempfile
import typing

from customizable_continuous_integration.automations.integration.test_commands.dbt_action import DBTAutomationActionCommand


class DBTAutomationTestCommand(DBTAutomationActionCommand):
    COMMAND_NAME: str = "DBTAutomationTestCommand"
    CLASS_NAME: str = "DBTAutomationTestCommand"

    def __init__(self, test_name: str, command_config: dict[typing.Any, typing.Any], throw_exception: bool = True) -> None:
        command_config["dbt_action"] = "test"
        super().__init__(test_name, command_config, throw_exception)
