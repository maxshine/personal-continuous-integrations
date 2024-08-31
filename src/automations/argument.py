"""This module defines the integration command line interface.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
"""

import argparse


def generate_arguments_parser() -> argparse.ArgumentParser:
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("--config_file", default="ci/resources/config/integration_test.yaml")
    return args_parser
