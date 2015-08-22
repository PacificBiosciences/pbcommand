import os
import unittest
import logging
import subprocess

from .base_utils import (HAS_PBCORE,
                         pbcore_skip_msg,
                         get_temp_file,
                         get_temp_dir)

from pbcommand.resolver import (resolve_tool_contract,
                                resolve_gather_tool_contract,
                                resolve_scatter_tool_contract)
from pbcommand.models import ResolvedToolContract, PipelineChunk
from pbcommand.pb_io import (load_tool_contract_from,
                             load_resolved_tool_contract_from)

from pbcommand.pb_io.tool_contract_io import write_resolved_tool_contract

log = logging.getLogger(__name__)


class PbTestApp(unittest.TestCase):

    """Generic Harness for running tool contracts end-to-end"""

    # if the base command is defined, DRIVER_EMIT and DRIVER_RESOLVE can be
    # guessed automatically
    DRIVER_BASE = None
    # complete Emit a tool contract
    DRIVER_EMIT = ""
    # Run tool from a resolve tool contract JSON file
    DRIVER_RESOLVE = ""

    # Requires Pbcore
    REQUIRES_PBCORE = False

    # input files that will be passed to the resolver
    # To get example files use, get_data_file("example.txt")]
    INPUT_FILES = []

    # Arguments passed to the Resolver
    MAX_NPROC = 1
    TASK_OPTIONS = {}

    # These will be checked against the resolved tool contract values
    RESOLVED_TASK_OPTIONS = {}
    RESOLVED_NPROC = 1

    @classmethod
    def setUpClass(cls):
        if cls.DRIVER_BASE is not None:
            if cls.DRIVER_EMIT == "":
                cls.DRIVER_EMIT = cls.DRIVER_BASE + " --emit-tool-contract "
            if cls.DRIVER_RESOLVE == "":
                cls.DRIVER_RESOLVE = cls.DRIVER_BASE + " --resolved-tool-contract "

    def _test_outputs_exists(self, rtc):
        """:type rtc: pbcommand.models.ResolvedToolContract"""
        log.debug("validating output file existence from {r}".format(r=rtc))
        log.debug("Resolved Output files from {t}".format(t=rtc.task.task_id))
        log.debug(rtc.task.output_files)
        for i, output_file in enumerate(rtc.task.output_files):
            msg = "Unable to find {i}-th output file {p}".format(i=i, p=output_file)
            self.assertTrue(os.path.exists(output_file), msg)

    def _to_rtc(self, tc, output_dir, tmp_dir):
        # handled the polymorphism in subclasses by overriding
        return resolve_tool_contract(tc, self.INPUT_FILES, output_dir, tmp_dir, self.MAX_NPROC, self.TASK_OPTIONS)

    def test_run_e2e(self):
        # hack to skip running the base Test class (which is the nose default behavior)
        if self.__class__.__name__ in ('PbTestApp', 'PbTestScatterApp', 'PbTestGatherApp'):
            return

        if self.REQUIRES_PBCORE:
            if not HAS_PBCORE:
                self.assertTrue(True, pbcore_skip_msg("Skipping running e2e for {d}".format(d=self.DRIVER_EMIT)))
                return

        output_dir = get_temp_dir(suffix="rtc-test")
        tmp_dir = get_temp_dir(suffix="rtc-temp")

        log.debug("Driver {e}".format(e=self.DRIVER_EMIT))
        log.debug("input files {i}".format(i=self.INPUT_FILES))
        log.debug("running in {p}".format(p=output_dir))

        output_tc = get_temp_file("-{n}-tool_contract.json".format(n=self.__class__.__name__), output_dir)
        emit_tc_exe = "{e} > {o}".format(e=self.DRIVER_EMIT, o=output_tc)
        rcode = subprocess.call([emit_tc_exe], shell=True)

        self.assertEquals(rcode, 0, "Emitting tool contract failed for '{e}'".format(e=emit_tc_exe))

        # sanity marshall-unmashalling
        log.debug("Loading tool-contract from {p}".format(p=output_tc))
        tc = load_tool_contract_from(output_tc)

        log.info("Resolving tool contract to RTC")

        rtc = self._to_rtc(tc, output_dir, tmp_dir)

        output_json_rtc = get_temp_file("resolved_tool_contract.json", output_dir)
        write_resolved_tool_contract(rtc, output_json_rtc)

        # sanity
        loaded_rtc = load_resolved_tool_contract_from(output_json_rtc)
        self.assertIsInstance(loaded_rtc, ResolvedToolContract)

        # Test Resolved options if specified.
        for opt, resolved_value in self.RESOLVED_TASK_OPTIONS.iteritems():
            self.assertTrue(opt in rtc.task.options, "Resolved option {x} not in RTC options.".format(x=opt))
            # this needs to support polymorphic equals (i.e., almostEquals
            if not isinstance(resolved_value, float):
                emsg = "Resolved option {o} are not equal. Expected {a}, got {b}".format(o=opt, b=rtc.task.options[opt], a=resolved_value)
                self.assertEquals(rtc.task.options[opt], resolved_value, emsg)

        # Resolved NPROC
        self.assertEquals(rtc.task.nproc, self.RESOLVED_NPROC)

        log.info("running resolved contract {r}".format(r=output_json_rtc))

        exe = "{d} {p}".format(p=output_json_rtc, d=self.DRIVER_RESOLVE)
        log.info("Running exe '{e}'".format(e=exe))
        rcode = subprocess.call([exe], shell=True)
        self.assertEqual(rcode, 0, "Running from resolved tool contract failed from {e}".format(e=exe))
        log.info("Successfully completed running e2e for {d}".format(d=self.DRIVER_EMIT))

        self._test_outputs_exists(rtc)

        self.run_after(rtc, output_dir)

    def run_after(self, rtc, output_dir):
        """
        Optional additional test code, e.g. to verify that the job produced
        the expected outputs.  This is run automatically by test_run_e2e, but
        does nothing unless overridden in a subclass.
        """
        pass


class PbTestScatterApp(PbTestApp):
    """Test harness for testing end-to-end scattering apps

    Override MAX_NCHUNKS, RESOLVED_MAX_NCHUNKS and CHUNK_KEYS
    """
    MAX_NCHUNKS = 12
    RESOLVED_MAX_NCHUNKS = 12
    CHUNK_KEYS = ()

    def _to_rtc(self, tc, output_dir, tmp_dir):
        return resolve_scatter_tool_contract(tc, self.INPUT_FILES, output_dir, tmp_dir, self.MAX_NPROC, self.TASK_OPTIONS, self.MAX_NCHUNKS, self.CHUNK_KEYS)


class PbTestGatherApp(PbTestApp):
    """Test harness for testing end-to-end gather apps

    Override the CHUNK_KEY to pass that into your resolver
    """
    CHUNK_KEY = PipelineChunk.CHUNK_KEY_PREFIX + 'fasta_id'

    def _to_rtc(self, tc, output_dir, tmp_dir):
        return resolve_gather_tool_contract(tc, self.INPUT_FILES, output_dir, tmp_dir, self.MAX_NPROC, self.TASK_OPTIONS, self.CHUNK_KEY)