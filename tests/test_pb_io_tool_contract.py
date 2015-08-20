import os
import unittest
import logging

from base_utils import get_data_file, HAS_PBCORE, pbcore_skip_msg, get_temp_file, get_temp_dir

from pbcommand.models import (ToolContract,
                              ResolvedToolContract,
                              MalformedToolContractError)

from pbcommand.pb_io.tool_contract_io import (load_tool_contract_from,
                                              load_resolved_tool_contract_from,
                                              write_resolved_tool_contract_avro)

import pbcommand.cli.examples.dev_app

log = logging.getLogger(__name__)


class TestLoadToolContract(unittest.TestCase):

    def test_01(self):
        file_name = "dev_example_tool_contract.json"
        path = get_data_file(file_name)
        tc = load_tool_contract_from(path)
        self.assertIsInstance(tc, ToolContract)


class TestMalformedToolContract(unittest.TestCase):

    def test_tc_no_inputs(self):
        file_name = "dev_example_tool_contract.json"
        path = get_data_file(file_name)
        tc = load_tool_contract_from(path)
        tc.task.input_file_types = []

        def _run():
            return tc.to_dict()

        self.assertRaises(MalformedToolContractError, _run)


class TestWriteResolvedToolContractAvro(unittest.TestCase):
    def test_01(self):
        file_name = "resolved_tool_contract_dev_app.json"
        rtc = load_resolved_tool_contract_from(get_data_file(file_name))
        self.assertIsInstance(rtc, ResolvedToolContract)

        d = get_temp_dir("rtc-app")
        f = get_temp_file("-resolved-tool-contract.avro", d)
        write_resolved_tool_contract_avro(rtc, f)
