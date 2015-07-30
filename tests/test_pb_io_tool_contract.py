import os
import unittest
import logging

from base_utils import get_data_file, HAS_PBCORE, pbcore_skip_msg
from pbcommand.pb_io.tool_contract_io import load_tool_contract_from


import pbcommand.cli.examples.dev_app

log = logging.getLogger(__name__)


class TestLoadToolContract(unittest.TestCase):

    def test_01(self):
        file_name = "dev_example_tool_contract.json"
        path = get_data_file(file_name)
        tc = load_tool_contract_from(path)
        self.assertIsNotNone(tc)
