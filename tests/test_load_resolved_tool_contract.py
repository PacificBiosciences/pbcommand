import pprint
import tempfile
import unittest
import logging
import os.path

from base_utils import get_data_file
from pbcommand.resolver import resolve_tool_contract
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
        name = "dev_example_dev_txt_app_tool_contract.json"
        p = get_data_file(name)
        tc = load_tool_contract_from(p)
        input_files = ["/tmp/file.txt"]
        root_output_dir = "/tmp"
        root_tmp_dir = root_output_dir
        tmp_file = tempfile.NamedTemporaryFile().name
        max_nproc = 2
        tool_options = {}
        rtc = resolve_tool_contract(tc, input_files, root_output_dir, root_tmp_dir, max_nproc, tool_options)
        log.info(pprint.pformat(rtc))
        self.assertIsNotNone(rtc)
        self.assertEqual(os.path.basename(rtc.task.output_files[0]),
            "output.txt")
        # Validate Resolved Resource Types
        log.debug("Resources {t}".format(t=rtc.task.resources))
        self.assertEqual(len(rtc.task.tmpdir_resources), 1)
        self.assertEqual(len(rtc.task.tmpfile_resources), 2)
        #self.assertEqual(rtc.task.tmp_file, tmp_file)
