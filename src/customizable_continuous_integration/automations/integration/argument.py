"""This module defines the integration command line interface.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
  31/10/2024   Ryan, Gao       Refactor the input parameter automation-config-file
"""

import argparse


def generate_arguments_parser() -> argparse.ArgumentParser:
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("--automation-config-file", default="")
    return args_parser
