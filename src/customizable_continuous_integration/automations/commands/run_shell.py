"""The entrypoint to run any shell command

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  04/09/2024   Ryan, Gao       Initial creation
  21/06/2025   Ryan, Gao       Add variadic parameters
"""

import logging
import sys

from pre_commit.util import cmd_output


def get_integration_test_logger() -> logging.Logger:
    _logger = logging.getLogger("shell_runner")
    logging_ch = logging.StreamHandler(sys.stdout)
    logging_ch.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(lineno)s: %(message)s"))
    _logger.addHandler(logging_ch)
    _logger.setLevel(logging.INFO)
    return _logger


_logger = get_integration_test_logger()


def split_command_str(cmd_str: str) -> list[str]:
    raw_cmds = [c for c in cmd_str.split(" ") if c]
    return raw_cmds


def run_shell_commands(cli_args: list[str], *args, **kargs) -> None:
    for cmd_str in cli_args:
        cmds = split_command_str(cmd_str)
        cmd, cmd_args = cmds[0], cmds[1:] if len(cmds) > 1 else []
        print(f"- Run: {cmd_str}")
        ret_code, std_out, std_err = cmd_output(cmd, *cmd_args)
        print(std_out)
        if ret_code != 0:
            print(f"std_error {std_err}")
            exit(1)
    exit(0)
