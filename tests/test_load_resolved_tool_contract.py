import os
import pprint
import unittest
import logging
import subprocess
import tempfile

from base_utils import get_data_file, HAS_PBCORE, pbcore_skip_msg, get_temp_file, \
    get_temp_dir

from pbcommand.cli.resolver import resolve_tool_contract
from pbcommand.pb_io.tool_contract_io import (load_resolved_tool_contract_from,
                                              load_tool_contract_from,
                                              write_resolved_tool_contract)

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


class TestLoadResolvedToolContract(_TestUtil):
    FILE_NAME = "resolved_contract_01.json"

    def _to_object(self, path):
        return load_resolved_tool_contract_from(path)


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
    file_name = "dev_example_tool_contract.json"
    path = get_data_file(file_name)

    @unittest.skipUnless(HAS_PBCORE, pbcore_skip_msg("Not running dev_app from resolved contract."))
    def test_01(self):

        d = get_temp_dir(suffix="rtc-test")
        log.debug("Running E-2-E in dev-app in {p}".format(p=d))

        tmp_fasta_file = get_temp_file("fasta", d)
        with open(tmp_fasta_file, 'w') as f:
            f.write(">record_48\nAACTTTCGGACCCGTGGTAGGATTGTGGGAGAATACTGTTGATGTTTTCAC\n")

        tc = load_tool_contract_from(self.path)

        log.info("Resolving tool contract to RTC")
        task_opts = {"pbcommand.task_options.dev_read_length": 27}
        rtc = resolve_tool_contract(tc, [tmp_fasta_file], d, d, 1, task_opts)

        output_json_rtc = os.path.join(d, "resolved_tool_contract.json")
        write_resolved_tool_contract(rtc, output_json_rtc)
        # sanity
        _ = load_resolved_tool_contract_from(output_json_rtc)

        log.info("running resolved contract {r}".format(r=self.path))

        exe = "python -m pbcommand.cli.examples.dev_app --resolved-tool-contract {p}".format(p=output_json_rtc)
        log.info("Running exe {e}".format(e=exe))
        rcode = subprocess.call([exe], shell=True)
        self.assertEqual(rcode, 0, "Running from resolved tool contract failed")

