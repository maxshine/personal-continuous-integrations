"""This module defines DBT Test automation command.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  30/10/2024   Ryan, Gao       Initial creation
"""

import os
import pathlib
import tempfile
import typing

from customizable_continuous_integration.automations.integration.test_commands.base import dbt_command


class DBTAutomationActionCommand(dbt_command.DBTAutomationBaseCommand):
    COMMAND_NAME: str = "DBTAutomationActionCommand"
    CLASS_NAME: str = "DBTAutomationActionCommand"

    def __init__(self, test_name: str, command_config: dict[typing.Any, typing.Any], throw_exception: bool = True) -> None:
        super().__init__(test_name, command_config, throw_exception)

    def do_execution(self, command_args: dict[typing.Any, typing.Any] = None) -> typing.Tuple[bool, str]:
        scoped_test_projects = self._command_config.get("target_projects", [])
        dbt_action = self._command_config.get("dbt_action")
        self._logger.info(f"Following DBT projects will be subject to the action {dbt_action}: \n{scoped_test_projects}")
        extra_args = [str(a) for a in command_args.values()] if command_args else []
        saved_cwd = os.getcwd()
        for prj in scoped_test_projects:
            os.chdir(saved_cwd)
            project_path = pathlib.Path(prj).resolve()
            dbt_exec_path = pathlib.Path(tempfile.mkdtemp())
            self._logger.info(f"Take action {dbt_action} project {prj} under {dbt_exec_path}")
            self.do_dbt_project_copy(project_path, dbt_exec_path)
            self.do_dbt_project_setup(self._command_config, dbt_exec_path)
            os.chdir(dbt_exec_path)
            self.do_dbt_run(dbt_action="deps", project_path=dbt_exec_path, profile_path=dbt_exec_path)
            test_result, result_message = self.do_dbt_run(
                dbt_action=dbt_action, project_path=dbt_exec_path, profile_path=dbt_exec_path, extra_args=extra_args
            )
            if not test_result:
                self._logger.error(f"Failed on project {prj}")
                return False, f"{self.test_name} on project {prj} failed with {result_message}"
        os.chdir(saved_cwd)
        return True, f"{self.test_name} Done"
