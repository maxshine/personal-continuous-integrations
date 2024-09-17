"""The entrypoint to the write protection hook

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  03/09/2024   Ryan, Gao       Initial creation
  17/09/2024   Ryan, Gao       Add cross-forks check
  30/09/2024   Ryan, Gao       Add support to use repo admin
"""

import argparse
import logging
import os
import pathlib
import sys

from pre_commit.commands.run import filter_by_include_exclude
from pre_commit.git import get_all_files
from pre_commit.lang_base import run_xargs
from pre_commit.util import CalledProcessError, cmd_output

from customizable_continuous_integration.common_libs.github_apis import repository
FORKED_REPOSITORY_REMOTE_NAME = "downstream"

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
    args_parser.add_argument("--forked-repository-url", default="", help="forked repository clone url")
    args_parser.add_argument("--github-access-token", default="", help="access token to access Github API")
    args_parser.add_argument("--github-repository-name", default="", help="full name of host repository")
    return args_parser


def get_write_protection_script_path() -> str:
    script_path = "write-protection-pr.sh"  # assume it is in the PATH
    this_file_path = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
    # in a project fs, otherwise do nothing because the project installed as a package.
    if "src/" in os.fspath(this_file_path.resolve()):
        target_cwd = this_file_path.parent.parent.parent.parent
        script_path = os.fspath(target_cwd / ".github" / "actions" / "write-protection-docker-action" / "write-protection-pr.sh")
    return script_path


def add_forked_repository(forked_repository_url, remote_name):
    cmd_output("git", "config", "--global", "--add", "safe.directory", os.getcwd())
    ret_code, ret_stdout, ret_stderr = cmd_output("git", "remote")
    if remote_name in ret_stdout:
        cmd_output("git", "remote", "remove", remote_name)
    cmd_output("git", "remote", "add", remote_name, forked_repository_url)
    cmd_output("git", "remote", "update", remote_name)


def write_protection_command(cli_args: list[str]) -> None:
    cmd = get_write_protection_script_path()
    args_parser = generate_arguments_parser()
    args = args_parser.parse_args(cli_args)

    head_ref = args.head_ref
    cmd_args = []
    if args.forked_repository_url and args.forked_repository_url.lower() != "unknown":
        add_forked_repository(forked_repository_url=args.forked_repository_url, remote_name=FORKED_REPOSITORY_REMOTE_NAME)
        cmd_args = ["-r"]
        if head_ref:
            head_ref = f"{FORKED_REPOSITORY_REMOTE_NAME}/{head_ref}"
    admin_list = args.admin_list
    if args.github_access_token and args.github_repository_name:
        admins = repository.get_repository_admins(args.github_access_token, args.github_repository_name)
        if admin_list:
            admins.append(admin_list)
        admin_list = ";".join(admins)

    if head_ref:
        cmd_args.extend(["-s", head_ref])
    if args.merge_ref:
        cmd_args.extend(["-t", args.merge_ref])
    if args.acting_user:
        cmd_args.extend(["-u", args.acting_user])
    if admin_list:
        cmd_args.extend(["-k", admin_list])
    try:
        git_files = list(filter_by_include_exclude(get_all_files(), args.include_filter, args.exclude_filter))
    except CalledProcessError:
        cmd_output("git", "config", "--global", "--add", "safe.directory", os.getcwd())
        git_files = list(filter_by_include_exclude(get_all_files(), args.include_filter, args.exclude_filter))

    if git_files:
        ret_code, std_out = run_xargs(cmd=(cmd, *cmd_args), file_args=git_files, require_serial=True, color=False)
        print(str(std_out, encoding="ascii"))
        if ret_code != 0:
            exit(1)
    exit(0)
