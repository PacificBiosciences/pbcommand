import pprint
import unittest
import logging
import os.path

from base_utils import get_data_file

from pbcommand.cli.resolver import resolve_tool_contract
from pbcommand.pb_io.tool_contract_io import (load_resolved_tool_contract_from,
                                              load_tool_contract_from)


log = logging.getLogger(__name__)


class _TestUtil(unittest.TestCase):
    FILE_NAME = "resolved_contract_01"

    def _to_object(self, path):
        log.debug("Loading from {p}".format(p=path))
        return load_tool_contract_from(path)

    def test_sanity(self):
        path = get_data_file(self.FILE_NAME)
        tool_contract = self._to_object(path)
        self.assertIsNotNone(tool_contract)


class TestLoadResolvedContract(unittest.TestCase):

    def test_01(self):
        path = get_data_file("dev_example_resolved_tool_contract.json")
        rtc = load_resolved_tool_contract_from(path)
        log.info(rtc)
        self.assertIsNotNone(rtc)


class TestResolveContract(unittest.TestCase):

    def test_01(self):
        name = "dev_example_tool_contract.json"
        p = get_data_file(name)
        tc = load_tool_contract_from(p)
        input_files = ["/tmp/file.txt"]
        root_output_dir = "/tmp"
        root_tmp_dir = root_output_dir
        max_nproc = 2
        rtc = resolve_tool_contract(tc, input_files, root_output_dir, root_tmp_dir, max_nproc, {})
        log.info(pprint.pformat(rtc))
        self.assertIsNotNone(rtc)
        self.assertEqual(os.path.basename(rtc.task.output_files[0]),
            "output.txt")
