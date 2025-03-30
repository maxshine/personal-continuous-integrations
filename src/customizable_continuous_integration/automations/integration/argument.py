"""This module defines the integration command line interface.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
  31/10/2024   Ryan, Gao       Refactor the input parameter automation-config-file
  28/03/2025   Ryan, Gao       Add default help argument
"""

import argparse


def generate_arguments_parser() -> argparse.ArgumentParser:
    args_parser = argparse.ArgumentParser(add_help=True)
    args_parser.add_argument("--automation-config-file", default="")
    return args_parser
