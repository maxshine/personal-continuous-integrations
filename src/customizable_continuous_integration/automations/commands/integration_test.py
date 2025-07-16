"""The entrypoint to the integration module

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  03/09/2024   Ryan, Gao       Initial creation
  31/10/2024   Ryan, Gao       Refactor the input parameter automation-config-file
  21/06/2025   Ryan, Gao       Add variadic parameters
  14/07/2025   Ryan, Gao       Add virtual environment setup and activation
"""

import os
import pathlib
import runpy
import shutil
import sys
import tempfile
import typing

import yaml
from pip._internal.cli.main import main as pip_main
from virtualenv.__main__ import run_with_catch as venv_run_with_catch

from customizable_continuous_integration.automations.integration.argument import generate_arguments_parser
from customizable_continuous_integration.automations.integration.logging import _logger


def create_venv(venv_config: dict[str, typing.Any]) -> str:
    venv_path = tempfile.mkdtemp()
    venv_args = ["--system-site-packages"]
    venv_args.extend([f"--python=python{sys.version_info.major}.{sys.version_info.minor}", venv_path])
    _logger.info(f"Setting up virtual environment {venv_path} with config: {venv_config} args: {venv_args}")
    venv_run_with_catch(args=venv_args)
    return venv_path


def activate_venv_info(venv_path: str, venv_config: dict[str, typing.Any]) -> dict[str, typing.Any]:
    py_env_backup = dict()
    py_env_backup["os_environ"] = os.environ.copy()
    py_env_backup["sys_path"] = sys.path.copy()
    py_env_backup["sys_prefix"] = sys.prefix
    py_env_backup["sys_real_prefix"] = sys.real_prefix if hasattr(sys, "real_prefix") else None
    venv_activate_this_file = f"{venv_path}/bin/activate_this.py"
    _logger.info(f"Activated virtual environment at {venv_path}")
    runpy.run_path(venv_activate_this_file)
    temp_req_file = tempfile.mktemp()
    with open(temp_req_file, "w") as f:
        for r in venv_config.get("requirements", []):
            _logger.info(f"Installing {r}")
            f.write(f"{r}\n")
    ret = pip_main(["install", "--require-virtualenv", "--ignore-installed", "--requirement", temp_req_file])
    if ret != 0:
        _logger.error(f"Failed to install {r} in virtual environment {venv_path}")
        raise RuntimeError(f"Failed to install {r} in virtual environment {venv_path}")
    return py_env_backup


def deactivate_venv_info(py_env_backup: dict[str, typing.Any]) -> None:
    os.environ = py_env_backup["os_environ"]
    sys.path = py_env_backup["sys_path"]
    sys.prefix = py_env_backup["sys_prefix"]
    if py_env_backup["sys_real_prefix"] is not None:
        sys.real_prefix = py_env_backup["sys_real_prefix"]


def integration_command(cli_args: list[str], *args, **kargs) -> None:
    args_parser = generate_arguments_parser()
    args = args_parser.parse_args(cli_args)
    ret = 0
    if args.automation_config_file:
        config_file_path = pathlib.Path(args.automation_config_file).resolve()
        with open(config_file_path, "r") as f:
            integration_test_config = yaml.safe_load(f)
        _logger.info(f"Integration test config:\n {integration_test_config}")
        py_env_backup = None
        venv_path = None
        if "virtual_environment" in integration_test_config:
            venv_path = create_venv(integration_test_config["virtual_environment"])
            py_env_backup = activate_venv_info(venv_path, integration_test_config["virtual_environment"])

        from customizable_continuous_integration.automations.integration.executor import execute_commands_in_serial
        ret = execute_commands_in_serial(integration_test_config)
        if "virtual_environment" in integration_test_config and py_env_backup is not None and venv_path is not None:
            deactivate_venv_info(py_env_backup)
            if not integration_test_config.get("keep_virtual_environment", False):
                _logger.info(f"Deleted virtual environment under {venv_path}")
                shutil.rmtree(venv_path)
            _logger.info(f"Deactivated virtual environment under {venv_path}")
    else:
        _logger.info("No test config file specified")
    exit(ret)
