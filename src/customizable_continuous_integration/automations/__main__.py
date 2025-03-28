"""This is the entrypoint to the automation module

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
  28/03/2025   Ryan, Gao       Add default help command
"""

import pathlib
import sys

from customizable_continuous_integration.automations.commands.constants import retrieve_cli_command


def main() -> None:
    cli_command = retrieve_cli_command(sys.argv[1]) if len(sys.argv) > 1 else retrieve_cli_command("help")
    unparsed_args = []
    if len(sys.argv) > 2:
        unparsed_args = sys.argv[2:]
    cli_command(unparsed_args)
    exit(0)


if __name__ == "__main__":
    main()
