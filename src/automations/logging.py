"""This module defines the integration logging interface.

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
"""

import logging
import sys


def get_main_logger() -> logging.Logger:
    _logger = logging.getLogger("integration_test_executor")
    logging_ch = logging.StreamHandler(sys.stdout)
    logging_ch.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(lineno)s: %(message)s"))
    _logger.addHandler(logging_ch)
    _logger.setLevel(logging.INFO)
    return _logger


_logger = get_main_logger()
