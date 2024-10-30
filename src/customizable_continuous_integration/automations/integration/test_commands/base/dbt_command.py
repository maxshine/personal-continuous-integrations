"""This module defines DBT automation base command.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
  01/11/2024   Ryan, Gao       Add workspace and configuration
"""

import io
import pathlib
import shutil
import typing

import yaml
from dbt.cli.main import dbtRunner, dbtRunnerResult

from customizable_continuous_integration.automations.integration.test_commands.base import base_command


class DBTAutomationBaseCommand(base_command.BaseAutomationCommand):
    COMMAND_NAME: str = "DBTAutomationBaseCommand"
    CLASS_NAME: str = "DBTAutomationBaseCommand"
    DBT_PROJECT_CONFIG_FILENAME = "dbt_project.yml"
    DBT_PROFILES_CONFIG_FILENAME = "profiles.yml"
    DBT_SOURCES_CONFIG_FILENAME = "sources.yml"
    DBT_PACKAGES_CONFIG_FILENAME = "packages.yml"

    DBT_TEST_CONFIG_FIELD_SOURCE = "dbt_source"
    DBT_TEST_CONFIG_FIELD_VARIABLE = "dbt_variable"
    DBT_TEST_CONFIG_FIELD_DEPENDENCY = "dbt_dependency"

    def __init__(self, test_name: str, command_config: dict[typing.Any, typing.Any], throw_exception: bool = True) -> None:
        super().__init__(test_name, command_config, throw_exception)

    def _yaml_dump_object_string(self, obj: typing.Any) -> str:
        """Return the string for input object in YAML.

        Args:
            obj(Any): A python object to be serialized
        Return:
            str: the YAML format serialized content
        Raises:
            None
        """
        str_io = io.StringIO()
        yaml.safe_dump(obj, str_io, indent=4)
        return str_io.getvalue()

    def _copy_directory(self, src: pathlib.Path, dst: pathlib.Path) -> None:
        """Do a directory-based copy.

        NOTE: it will remove the dst path if it exists in prior
        Args:
            src (Path): The source directory having files to be copied
            dst (Path): The destination directory to receive files
        Return:
            None
        Raises:
            None
        """
        src_path = src.absolute()
        dst_path = dst.absolute()
        if not src_path.is_dir():
            self._logger.error(f"{src} is NOT a directory path, nothing to do")
            return
        if dst_path.exists() and dst_path.is_dir():
            self._logger.info("destination is a directory to be deleted")
            shutil.rmtree(dst_path)
        elif dst_path.exists():
            self._logger.info("destination is a file to be deleted")
            dst_path.unlink()
        self._logger.info(f"copying {src} to {dst}")
        shutil.copytree(src_path, dst_path)

    def do_dbt_project_copy(self, dbt_source_path: pathlib.Path, dbt_target_path: pathlib.Path) -> None:
        """Copy DBT project files to target path.

        NOTE: it will remove the dbt_target_path if it exists in prior
        Args:
            dbt_source_path (str): The source directory having files to be copied
            dbt_target_path (str): The destination directory to receive files
        Return:
            None
        Raises:
            None
        """
        self._logger.info(f"To prepare default dbt into execution path {dbt_target_path}")
        self._copy_directory(dbt_source_path, dbt_target_path)

    def do_dbt_project_setup(self, dbt_test_config: dict, dbt_target_path: pathlib.Path):
        """Setup DBT config according optional config from the test config

        NOTE: it will remove the dbt_target_path if it exists in prior
        Args:
            dbt_test_config (dict): The dbt test configuration
            dbt_target_path (str): The destination directory to receive files
        Return:
            None:
        Raises:
            None
        """

        if self.DBT_TEST_CONFIG_FIELD_VARIABLE in dbt_test_config:
            with open(dbt_target_path / self.DBT_PROJECT_CONFIG_FILENAME, "r") as f:
                dbt_project_obj = yaml.safe_load(f)
            with open(dbt_target_path / self.DBT_PROJECT_CONFIG_FILENAME, "w") as f:
                dbt_project_obj["vars"] = dbt_test_config[self.DBT_TEST_CONFIG_FIELD_VARIABLE]
                content = self._yaml_dump_object_string(dbt_project_obj)
                self._logger.info(f"DBT project config content: \n{content}")
                f.write(content)
        if self.DBT_TEST_CONFIG_FIELD_SOURCE in dbt_test_config:
            with open(dbt_target_path / "models" / self.DBT_SOURCES_CONFIG_FILENAME, "w") as f:
                content = self._yaml_dump_object_string({"sources": dbt_test_config[self.DBT_TEST_CONFIG_FIELD_SOURCE]})
                self._logger.info(f"DBT source config content: \n{content}")
                f.write(content)
        if self.DBT_TEST_CONFIG_FIELD_DEPENDENCY in dbt_test_config:
            with open(dbt_target_path / self.DBT_PACKAGES_CONFIG_FILENAME, "w") as f:
                content = self._yaml_dump_object_string({"packages": dbt_test_config[self.DBT_TEST_CONFIG_FIELD_DEPENDENCY]})
                self._logger.info(f"DBT packages config content: \n{content}")
                f.write(content)

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
