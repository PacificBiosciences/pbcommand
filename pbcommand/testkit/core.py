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
    # To get example files use, get_data_file("example.txt")]
    INPUT_FILES = []
    TASK_OPTIONS = {}
    MAX_NPROC = 1

    def test_run_e2e(self):
        if self.REQUIRES_PBCORE:
            if not HAS_PBCORE:
                self.assertTrue(True, pbcore_skip_msg("Skipping running e2e for {d}".format(d=self.DRIVER_EMIT)))
                return

        output_dir = get_temp_dir(suffix="rtc-test")
        tmp_dir = get_temp_dir(suffix="rtc-temp")

        log.debug("Driver {e}".format(e=self.DRIVER_EMIT))
        log.debug("input files {i}".format(i=self.INPUT_FILES))
        log.debug("running in {p}".format(p=output_dir))

        output_tc = get_temp_file("dev_example_tool_contract.json", output_dir)
        emit_tc_exe = "{e} > {o}".format(e=self.DRIVER_EMIT, o=output_tc)
        rcode = subprocess.call([emit_tc_exe], shell=True)

        self.assertEquals(rcode, 0, "Emitting tool contract failed for '{e}'".format(e=emit_tc_exe))

        # sanity marshall-unmashalling
        log.debug("Loading tool-contract from {p}".format(p=output_tc))
        tc = load_tool_contract_from(output_tc)

        log.info("Resolving tool contract to RTC")

        rtc = resolve_tool_contract(tc, self.INPUT_FILES, output_dir, tmp_dir, self.MAX_NPROC, self.TASK_OPTIONS)

        output_json_rtc = get_temp_file("resolved_tool_contract.json", output_dir)
        write_resolved_tool_contract(rtc, output_json_rtc)

        # sanity
        _ = load_resolved_tool_contract_from(output_json_rtc)

        log.info("running resolved contract {r}".format(r=output_json_rtc))

        exe = "{d} {p}".format(p=output_json_rtc, d=self.DRIVER_RESOLVE)
        log.info("Running exe '{e}'".format(e=exe))
        rcode = subprocess.call([exe], shell=True)
        self.assertEqual(rcode, 0, "Running from resolved tool contract failed from {e}".format(e=exe))
        log.info("Successfully completed running e2e for {d}".format(d=self.DRIVER_EMIT))
        self.run_after(rtc, output_dir)

    def run_after(self, rtc, output_dir):
        """
        Optional additional test code, e.g. to verify that the job produced
        the expected outputs.  This is run automatically by test_run_e2e, but
        does nothing unless overridden in a subclass.
        """
        pass
