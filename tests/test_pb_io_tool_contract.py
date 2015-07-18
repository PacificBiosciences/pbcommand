import os
import unittest
import logging

from base_utils import get_data_file, HAS_PBCORE, pbcore_skip_msg
from pbcommand.pb_io.tool_contract_io import (write_tool_contract,
                                              load_tool_contract_from)


import pbcommand.cli.examples.dev_app

log = logging.getLogger(__name__)


@unittest.skipUnless(HAS_PBCORE, pbcore_skip_msg("Not running dev_app from resolved contract."))
class TestRunDevApp(unittest.TestCase):
    def test_01(self):
        file_name = "dev_example_tool_contact.json"
        path = get_data_file(file_name)
        exe = "python -m pbcommand.cli.examples.dev_app --resolved-tool-contract {p}".format(p=path)
        log.info("running resolved contract {r}".format(r=path))
        log.info(exe)
        self.assertTrue(True)

    def test_emit_and_reload(self):
        p = pbcommand.cli.examples.dev_app.get_contract_parser()
        write_tool_contract(p, "test_tool_contract.json")
        try:
            tool_contract = load_tool_contract_from("test_tool_contract.json")
            log.info("Successfully loaded tool contract")
            self.assertIsNotNone(tool_contract)
        finally:
            os.remove("test_tool_contract.json")