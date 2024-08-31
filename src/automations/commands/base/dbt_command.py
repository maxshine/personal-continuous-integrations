"""This module defines DBT automation base command.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
"""

import pathlib
import typing

from dbt.cli.main import dbtRunner, dbtRunnerResult

from automations.commands.base import base_command


class DBTAutomationBaseCommand(base_command.BaseAutomationCommand):
    COMMAND_NAME: str = "DBTAutomationBaseCommand"
    CLASS_NAME: str = "DBTAutomationBaseCommand"

    def __init__(self, test_name: str, command_config: dict[typing.Any, typing.Any], throw_exception: bool = True) -> None:
        super().__init__(test_name, command_config, throw_exception)

    def do_dbt_run(
        self, dbt_action: str, project_path: pathlib.Path, profile_path: pathlib.Path, extra_args: list[str] = None
    ) -> typing.Tuple[bool, str]:
        project_path = project_path.resolve()
        profile_path = profile_path.resolve()
        self._logger.info(f"Run {dbt_action} against project under {project_path}")
        test_args = [dbt_action, f"--project-dir={project_path}", f"--profiles-dir={profile_path}"]
        test_args.extend(extra_args if extra_args else [])
        dbt_runner = dbtRunner()
        test_result: dbtRunnerResult = dbt_runner.invoke(test_args)
        if not test_result.success:
            return False, f"{test_result.exception}"
        return True, f"{self.test_name} Done"

    def do_execution(self, command_args: dict[typing.Any, typing.Any] = None) -> typing.Tuple[bool, str]:
        raise NotImplementedError()
