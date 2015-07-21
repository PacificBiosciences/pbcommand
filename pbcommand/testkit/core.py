import os
import unittest
import logging
import subprocess

from .base_utils import (HAS_PBCORE,
                         pbcore_skip_msg,
                         get_temp_file,
                         get_temp_dir)

from pbcommand.cli import resolve_tool_contract
from pbcommand.pb_io import (load_tool_contract_from,
                             load_resolved_tool_contract_from)

from pbcommand.pb_io.tool_contract_io import write_resolved_tool_contract

log = logging.getLogger(__name__)

class PbTestApp(unittest.TestCase):
    """Generic Harness for running tool contracts end-to-end"""

    # complete Emit a tool contract
    DRIVER_EMIT = ""
    # complete resolve tool contract
    DRIVER_RESOLVE = ""

    # Requires Pbcore
    REQUIRES_PBCORE = False

    # input files that will be passed to the resolver
    INPUT_FILES = []#get_data_file("example.txt")]
    TASK_OPTIONS = {}
    MAX_NPROC = 1

    def test_run_e2e(self):
        if self.REQUIRES_PBCORE:
            if not HAS_PBCORE:
                self.assertTrue(True, "Skipping running e2e for {d}".format(d=self.DRIVER_EMIT))
                return

        d = get_temp_dir(suffix="rtc-test")
        log.debug("Driver {e}".format(e=self.DRIVER_EMIT))
        log.debug("input files {i}".format(i=self.INPUT_FILES))
        log.debug("running in {p}".format(p=d))

        output_tc = get_temp_file("dev_example_tool_contract.json", d)
        emit_tc_exe = "{e} > {o}".format(e=self.DRIVER_EMIT, o=output_tc)
        rcode = subprocess.call([emit_tc_exe], shell=True)

        self.assertEquals(rcode, 0, "Emitting tool contract failed for '{e}'".format(e=emit_tc_exe))

        # sanity marshall-unmashalling
        log.debug("Loading tool-contract from {p}".format(p=output_tc))
        tc = load_tool_contract_from(output_tc)

        log.info("Resolving tool contract to RTC")

        rtc = resolve_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, self.TASK_OPTIONS)

        output_json_rtc = get_temp_file("resolved_tool_contract.json", d)
        write_resolved_tool_contract(rtc, output_json_rtc)

        # sanity
        _ = load_resolved_tool_contract_from(output_json_rtc)

        log.info("running resolved contract {r}".format(r=output_json_rtc))

        exe = "{d} {p}".format(p=output_json_rtc, d=self.DRIVER_RESOLVE)
        log.info("Running exe '{e}'".format(e=exe))
        rcode = subprocess.call([exe], shell=True)
        self.assertEqual(rcode, 0, "Running from resolved tool contract failed from {e}".format(e=exe))
        log.info("Successfully:wq completed running e2e for {d}".format(d=self.DRIVER_EMIT))
