"""The entrypoint to the write protection hook

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  03/09/2024   Ryan, Gao       Initial creation
"""

import argparse
import logging
import os
import pathlib
import sys

from pre_commit.commands.run import filter_by_include_exclude
from pre_commit.git import get_all_files
from pre_commit.util import cmd_output


def get_integration_test_logger() -> logging.Logger:
    _logger = logging.getLogger("write_protection_hook")
    logging_ch = logging.StreamHandler(sys.stdout)
    logging_ch.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(lineno)s: %(message)s"))
    _logger.addHandler(logging_ch)
    _logger.setLevel(logging.INFO)
    return _logger


_logger = get_integration_test_logger()


def generate_arguments_parser() -> argparse.ArgumentParser:
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("--head-ref", default="", help="head reference in the pull request")
    args_parser.add_argument("--merge-ref", default="", help="merge reference in the pull request")
    args_parser.add_argument("--acting-user", default="", help="github user id initiating this action")
    args_parser.add_argument("--include-filter", default="^$", help="reg filter for files inclusion")
    args_parser.add_argument("--exclude-filter", default="^$", help="reg filter for files exclusion")
    args_parser.add_argument("--admin-list", default="", help="reg filter for files exclusion")
    return args_parser


def get_write_protection_script_path() -> str:
    script_path = "write-protection-pr.sh"  # assume it is in the PATH
    this_file_path = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    # in a project fs, otherwise do nothing because the project installed as a package.
    if "src/" in os.fspath(this_file_path.resolve()):
        target_cwd = this_file_path.parent.parent.parent
        script_path = os.fspath(target_cwd / "scripts" / "write-protection-pr.sh")
    return script_path


def write_protection_command(cli_args: list[str]) -> None:
    cmd = get_write_protection_script_path()
    args_parser = generate_arguments_parser()
    args = args_parser.parse_args(cli_args)
    cmd_args = []
    if args.head_ref:
        cmd_args.extend(["-s", args.head_ref])
    if args.merge_ref:
        cmd_args.extend(["-t", args.merge_ref])
    if args.acting_user:
        cmd_args.extend(["-u", args.acting_user])
    if args.admin_list:
        cmd_args.extend(["-k", args.admin_list])
    git_files = list(filter_by_include_exclude(get_all_files(), args.include_filter, args.exclude_filter))
    if git_files:
        ret_code, std_out, std_err = cmd_output(cmd, *cmd_args, *git_files)
        print(std_out)
        if ret_code != 0:
            _logger.error(std_err)
            exit(1)
    exit(0)
