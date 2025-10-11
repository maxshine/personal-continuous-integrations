"""This module defines the integration executor.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
  31/10/2024   Ryan, Gao       Refactor the config schema with `automations` field
  04/11/2024   Ryan, Gao       refactor the name of `automation_config` and `automation_args`
  09/11/2024   Ryan, Gao       fix the false positive when `continue_on_failure`
  14/07/2025   Ryan, Gao       Add return code in test command execution
"""

import os
import pathlib
import typing
from concurrent.futures import ProcessPoolExecutor, as_completed

from customizable_continuous_integration.automations.integration.logging import _logger
from customizable_continuous_integration.automations.integration.test_commands.constants import SentinelCommand, retrieve_test_command

SUCCESS = 0
TEST_FAILED = 1


def is_github_environment() -> bool:
    return any([True if var_name.startswith("GITHUB_") else False for var_name in os.environ])


def prepare_test_environment() -> None:
    this_file_path = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    target_cwd = this_file_path.parent.parent.parent.parent
    # in a project fs of non-GitHub environment, otherwise do nothing because the project installed as a package.
    if "src/" in os.fspath(this_file_path.resolve()) and not is_github_environment():
        _logger.info(f"Switch to working directory {target_cwd}")
        os.chdir(target_cwd.resolve())


def do_execute_command(test_name: str, command_config: dict[typing.Any, typing.Any], continue_on_failure: bool) -> (bool, str):
    prepare_test_environment()
    command_name = command_config["command"]
    command_impl = retrieve_test_command(command_name)
    if command_impl is SentinelCommand:
        _logger.warning(f"Skipping command {command_name} because it is not registered")
    test_instance = command_impl(
        test_name=test_name, command_config=command_config["automation_config"], throw_exception=command_config["throw_exception"]
    )
    _logger.info(f"Start executing test {test_name} with {test_instance}")
    try:
        ret, ret_msg = test_instance.execute(command_config.get("automation_args", []))
        return ret, ret_msg
    except Exception as e:
        _logger.error(f"{test_name} FAILED with exception: {e}")
        return False, f"{test_name} FAILED with exception: {e}"


def execute_command_worker(worker_id: int, test_name: str, command_config: dict[typing.Any, typing.Any], continue_on_failure=False) -> (bool, str):
    _logger.info(f"[Worker-{worker_id}] Start executing test {test_name}")
    return do_execute_command(test_name=test_name, command_config=command_config, continue_on_failure=continue_on_failure)


def execute_commands_in_process(integration_test_config: dict[typing.Any, typing.Any]) -> int:
    configured_tests = integration_test_config.get("automations", [])
    continue_on_failure = integration_test_config.get("continue_on_failure", False)
    concurrency = integration_test_config.get("concurrency", 1)
    task_requests = {}
    failed_tasks_results = {}
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
                    _logger.error(f"{test_name} FAILED: {ret_msg}, execution will be continued")
                    failed_tasks_results[test_name] = (ret, ret_msg)
                else:
                    _logger.error(f"{test_name} FAILED: {ret_msg}, execution will be stopped")
                    executor.shutdown(wait=False, cancel_futures=True)
                    return TEST_FAILED
            except Exception as e:
                _logger.error(f"{test_name} FAILED with exception: {e}, execution will be stopped")
                executor.shutdown(wait=False, cancel_futures=True)
                return TEST_FAILED
    if failed_tasks_results:
        _logger.error(f"These automations FAILED: {list(failed_tasks_results.keys())}")
        return TEST_FAILED
    return SUCCESS


def execute_commands_in_serial(integration_test_config: dict[typing.Any, typing.Any]) -> int:
    configured_tests = integration_test_config.get("automations", [])
    continue_on_failure = integration_test_config.get("continue_on_failure", False)
    failed_tasks_results = {}
    cwd = os.getcwd()
    for test_name in configured_tests:
        _logger.info(f"Starting test {test_name} from working directory {cwd}")
        os.chdir(cwd)
        command_config = configured_tests[test_name]
        ret, ret_msg = do_execute_command(test_name=test_name, command_config=command_config, continue_on_failure=continue_on_failure)
        if ret:
            _logger.info(f"{test_name} PASSED: {ret_msg}")
        elif continue_on_failure:
            _logger.error(f"{test_name} FAILED: {ret_msg}, execution will be continued")
            failed_tasks_results[test_name] = (ret, ret_msg)
        else:
            _logger.error(f"{test_name} FAILED: {ret_msg}")
            return TEST_FAILED
        prepare_test_environment()
    if failed_tasks_results:
        _logger.error(f"These automations FAILED: {list(failed_tasks_results.keys())}")
        return TEST_FAILED
    return SUCCESS
