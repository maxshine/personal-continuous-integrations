"""This module defines DBT Test automation command.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
"""

import os
import pathlib
import typing

from automations.commands.base import dbt_command


class DBTAutomationTestCommand(dbt_command.DBTAutomationBaseCommand):
    COMMAND_NAME: str = "DBTAutomationTestCommand"
    CLASS_NAME: str = "DBTAutomationTestCommand"

    def __init__(self, test_name: str, command_config: dict[typing.Any, typing.Any], throw_exception: bool = True) -> None:
        super().__init__(test_name, command_config, throw_exception)

    def do_execution(self, command_args: dict[typing.Any, typing.Any] = None) -> typing.Tuple[bool, str]:
        scoped_test_projects = self._command_config.get("test_projects", [])
        self._logger.info(f"Following DBT projects will be subject to the test: \n{scoped_test_projects}")
        extra_args = [str(a) for a in command_args.values()] if command_args else []
        for prj in scoped_test_projects:
            project_path = pathlib.Path(prj).resolve()
            self._logger.info(f"Testing project {prj} under {project_path}")
            test_result, result_message = self.do_dbt_run(
                dbt_action="test", project_path=project_path, profile_path=project_path, extra_args=extra_args
            )
            if not test_result:
                self._logger.error(f"Failed on project {prj}")
                return False, f"{self.test_name} on project {prj} failed with {result_message}"
        return True, f"{self.test_name} Done"
