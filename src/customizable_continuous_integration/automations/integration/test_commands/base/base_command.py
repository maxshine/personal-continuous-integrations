"""This module defines base automation command class

Author:
  Ryan,Gao (ryangao-au@outlook.com)
Revision History:
  Date         Author		   Comments
------------------------------------------------------------------------------
  27/08/2024   Ryan, Gao       Initial creation
"""

import logging
import sys
import typing


class BaseAutomationCommand:
    COMMAND_NAME: str = "BaseAutomationCommand"
    CLASS_NAME: str = "BaseAutomationCommand"

    def __init__(self, test_name: str, command_config: dict[typing.Any, typing.Any], throw_exception: bool = True) -> None:
        self._test_name = test_name
        self._command_config: dict[typing.Any, typing.Any] = command_config
        self._throw_exception = throw_exception
        self._logger = logging.getLogger(self.CLASS_NAME)
        self._logger.setLevel(logging.INFO)
        if not self._logger.hasHandlers():
            logging_ch = logging.StreamHandler(sys.stdout)
            logging_ch.setFormatter(logging.Formatter("%(asctime)s %(name)s %(levelname)s %(lineno)s: %(message)s"))
            self._logger.addHandler(logging_ch)
            self._logger.setLevel(logging.INFO)

    def do_execution(self, command_args: dict[typing.Any, typing.Any] = None) -> typing.Tuple[bool, str]:
        return True, ""

    def setup(self, command_config: dict[typing.Any, typing.Any] = None) -> None:
        pass

    def tear_up(self, command_args: dict[typing.Any, typing.Any] = None) -> None:
        pass

    def execute(self, command_args: dict[typing.Any, typing.Any] = None) -> typing.Tuple[bool, str]:
        try:
            self.setup(self._command_config)
            _ret, _ret_msg = self.do_execution(command_args)
        except Exception as e:
            _ret, _ret_msg = False, str(e)
            if self._throw_exception:
                raise e
        finally:
            self.tear_up(self._command_config)
        return _ret, _ret_msg

    @property
    def test_name(self) -> str:
        return self._test_name

    def __repr__(self):
        return f"{self._test_name}({self.COMMAND_NAME})"

    def __str__(self):
        return f"{self._test_name}({self.COMMAND_NAME})"
