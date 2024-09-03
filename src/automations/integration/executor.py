"""This module defines the integration executor.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
"""

import os
import pathlib
import sys
import typing
from concurrent.futures import ProcessPoolExecutor, as_completed

from automations.integration.logging import _logger
from automations.integration.test_commands.constants import SentinelCommand, retrieve_test_command


def prepare_test_environment() -> None:
    this_file_path = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    target_cwd = this_file_path.parent.parent.parent
    # in a project fs, otherwise do nothing because the project installed as a package.
    if "src/" in os.fspath(target_cwd.resolve()):
        _logger.info(f"Switch to working directory {target_cwd}")
        os.chdir(target_cwd.resolve())


def do_execute_command(test_name: str, command_config: dict[typing.Any, typing.Any], continue_on_failure: bool) -> (bool, str):
    prepare_test_environment()
    command_name = command_config["command"]
    command_impl = retrieve_test_command(command_name)
    if command_impl is SentinelCommand:
        _logger.warning(f"Skipping command {command_name} because it is not registered")
    test_instance = command_impl(test_name=test_name, command_config=command_config["test_config"], throw_exception=command_config["throw_exception"])
    _logger.info(f"Start executing test {test_name} with {test_instance}")
    try:
        ret, ret_msg = test_instance.execute(command_config.get("test_args", {}))
        return ret, ret_msg
    except Exception as e:
        _logger.info(f"{test_name} FAILED with exception: {e}")
        if continue_on_failure:
            return False, f"{test_name} FAILED with exception: {e}"
        else:
            raise e


def execute_command_worker(worker_id: int, test_name: str, command_config: dict[typing.Any, typing.Any], continue_on_failure=False) -> (bool, str):
    _logger.info(f"[Worker-{worker_id}] Start executing test {test_name}")
    return do_execute_command(test_name=test_name, command_config=command_config, continue_on_failure=continue_on_failure)


def execute_commands_in_process(integration_test_config: dict[typing.Any, typing.Any]) -> None:
    configured_tests = integration_test_config.get("tests", [])
    continue_on_failure = integration_test_config.get("continue_on_failure", False)
    concurrency = integration_test_config.get("concurrency", 1)
    task_requests = {}
    with ProcessPoolExecutor(max_workers=concurrency) as executor:
        for idx, test_name in enumerate(configured_tests):
            task_req = (idx, test_name, configured_tests[test_name].copy(), continue_on_failure)
            task_requests[executor.submit(execute_command_worker, *task_req)] = task_req
        for completed_task in as_completed(task_requests):
            completed_task_req = task_requests[completed_task]
            try:
                ret, ret_msg = completed_task.result()
                if ret:
                    _logger.info(f"{completed_task_req[1]} PASSED: {ret_msg}")
                elif continue_on_failure:
                    _logger.info(f"{test_name} FAILED: {ret_msg}, execution will be continued")
                else:
                    _logger.info(f"{test_name} FAILED: {ret_msg}, execution will be stopped")
                    executor.shutdown(wait=False, cancel_futures=True)
                    exit(1)
            except Exception as e:
                _logger.info(f"{test_name} FAILED with exception: {e}, execution will be stopped")
                executor.shutdown(wait=False, cancel_futures=True)
                exit(1)


def execute_commands_in_serial(integration_test_config: dict[typing.Any, typing.Any]) -> None:
    configured_tests = integration_test_config.get("tests", [])
    continue_on_failure = integration_test_config.get("continue_on_failure", False)
    for test_name in configured_tests:
        command_config = configured_tests[test_name]
        ret, ret_msg = do_execute_command(test_name=test_name, command_config=command_config, continue_on_failure=continue_on_failure)
        if ret:
            _logger.info(f"{test_name} PASSED: {ret_msg}")
        elif continue_on_failure:
            _logger.info(f"{test_name} FAILED: {ret_msg}, execution will be continued")
        else:
            _logger.info(f"{test_name} FAILED: {ret_msg}")
            exit(1)
        prepare_test_environment()
