from __future__ import absolute_import
import unittest
import logging

from base_utils import get_tool_contract_v1

from pbcommand.models import (ToolContract,
                              MalformedToolContractError)

from pbcommand.pb_io.tool_contract_io import (load_tool_contract_from, )


log = logging.getLogger(__name__)


class TestLoadToolContract(unittest.TestCase):

    def test_01(self):
        file_name = "dev_example_tool_contract.json"
        path = get_tool_contract_v1(file_name)
        tc = load_tool_contract_from(path)
        self.assertIsInstance(tc, ToolContract)
        self.assertEqual(tc.schema_version, "UNKNOWN")


class TestMalformedToolContract(unittest.TestCase):

    def test_tc_no_inputs(self):
        file_name = "dev_example_tool_contract.json"
        path = get_tool_contract_v1(file_name)
        tc = load_tool_contract_from(path)
        tc.task.input_file_types = []

        def _run():
            return tc.to_dict()

        self.assertRaises(MalformedToolContractError, _run)
