import pprint
import unittest
import logging

from base_utils import get_data_file

from pbcommand.cli.driver import resolve_tool_contract
from pbcommand.pb_io.tool_contract_io import (load_resolved_tool_contract_from,
                                              load_tool_contract_from)

log = logging.getLogger(__name__)

_HAS_PBCORE = False

try:
    import pbcore
    _HAS_PBCORE = True
except ImportError:
    pass


class _TestUtil(unittest.TestCase):
    FILE_NAME = "resolved_contract_01"

    def _to_object(self, path):
        return load_tool_contract_from(path)

    def test_sanity(self):
        path = get_data_file(self.FILE_NAME)
        tool_contract = self._to_object(path)
        self.assertIsNotNone(tool_contract)


class TestLoadResolvedToolContract(_TestUtil):
    FILE_NAME = "resolved_contract_01.json"

    def _to_object(self, path):
        return load_resolved_tool_contract_from(path)


class TestLoadToolContract(_TestUtil):
    FILE_NAME = "tool_contract_01.json"

    def _to_object(self, path):
        return load_tool_contract_from(path)


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


class TestRunDevApp(unittest.TestCase):
    file_name = "dev_example_tool_contact.json"
    path = get_data_file(file_name)

    @unittest.skipIf(_HAS_PBCORE, "pbcore is not installed. Not running dev_app from resolved contract.")
    def test_01(self):
        exe = "python -m pbcommand.cli.examples.dev_app --resolved-tool-contract {p}".format(p=self.path)
        log.info("running resolved contract {r}".format(r=self.path))
        log.info(exe)
        self.assertTrue(True)
