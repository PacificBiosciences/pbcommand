import logging
import unittest

from base_utils import (get_temp_dir, get_tool_contract, get_resolved_tool_contract)
from pbcommand.models import ResolvedToolContract, ResolvedToolContractTask, ResolvedScatteredToolContractTask, ResolvedGatherToolContractTask

from pbcommand.pb_io import load_tool_contract_from
from pbcommand.resolver import resolve_tool_contract, resolve_scatter_tool_contract, resolve_gather_tool_contract, ToolContractError

log = logging.getLogger(__name__)


class TestScatterResolver(unittest.TestCase):
    FILE_NAME = "dev_scatter_fasta_app_tool_contract.json"
    MAX_NCHUNKS = 7
    MAX_NPROC = 9
    INPUT_FILES = ['/tmp/file.fasta']
    CHUNK_KEYS = ('$chunk.fasta_id')

    TOOL_OPTIONS = {}

    def test_sanity(self):
        d = get_temp_dir("resolved-tool-contract")
        tc = load_tool_contract_from(get_tool_contract(self.FILE_NAME))
        rtc = resolve_scatter_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, self.TOOL_OPTIONS, self.MAX_NCHUNKS, self.CHUNK_KEYS, False)
        self.assertIsInstance(rtc, ResolvedToolContract)
        self.assertIsInstance(rtc.task, ResolvedScatteredToolContractTask)
        self.assertEqual(rtc.task.max_nchunks, 7)
        self.assertEqual(rtc.task.is_distributed, False)


class TestGatherResolver(unittest.TestCase):
    FILE_NAME = "dev_gather_fasta_app_tool_contract.json"
    MAX_NCHUNKS = 7
    MAX_NPROC = 9
    INPUT_FILES = ['/tmp/file.fasta.chunk.json']
    CHUNK_KEY = '$chunk.filter_fasta_id'

    TOOL_OPTIONS = {}

    def test_sanity(self):
        d = get_temp_dir("resolved-tool-contract")
        tc = load_tool_contract_from(get_tool_contract(self.FILE_NAME))
        rtc = resolve_gather_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, self.TOOL_OPTIONS, self.CHUNK_KEY, False)
        self.assertIsInstance(rtc, ResolvedToolContract)
        self.assertIsInstance(rtc.task, ResolvedGatherToolContractTask)
        self.assertEqual(rtc.task.chunk_key, self.CHUNK_KEY)
        self.assertEqual(rtc.task.is_distributed, False)


def _to_id(i):
    return "pbcommand.task_options.{i}".format(i=i)


class TestResolver(unittest.TestCase):
    FILE_NAME = "dev_mixed_app_tool_contract.json"
    MAX_NPROC = 1
    INPUT_FILES = ['/tmp/file.csv']
    PLOIDY = _to_id("ploidy")
    ALPHA = _to_id("alpha")
    BETA = _to_id("beta")
    GAMMA = _to_id("gamma")

    def test_sanity(self):
        d = get_temp_dir("resolved-tool-contract")
        tc = load_tool_contract_from(get_tool_contract(self.FILE_NAME))
        tool_options = {}
        rtc = resolve_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, tool_options, False)
        self.assertIsInstance(rtc, ResolvedToolContract)
        self.assertIsInstance(rtc.task, ResolvedToolContractTask)
        self.assertEqual(rtc.task.is_distributed, False)
        self.assertEqual(rtc.task.options[self.ALPHA], 25)
        self.assertEqual(rtc.task.options[self.BETA], 1.234)
        self.assertEqual(rtc.task.options[self.GAMMA], True)
        self.assertEqual(rtc.task.options[self.PLOIDY], "haploid")
        # non-defaults
        tool_options = {self.ALPHA: 15, self.BETA: 2.5, self.GAMMA: False, self.PLOIDY: "diploid"}
        rtc = resolve_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, tool_options, False)
        self.assertEqual(rtc.task.options[self.ALPHA], 15)
        self.assertEqual(rtc.task.options[self.BETA], 2.5)
        self.assertEqual(rtc.task.options[self.GAMMA], False)
        self.assertEqual(rtc.task.options[self.PLOIDY], "diploid")

    def test_failure_modes(self):
        d = get_temp_dir("resolved-tool-contract")
        tc = load_tool_contract_from(get_tool_contract(self.FILE_NAME))
        tool_options = {self.PLOIDY: "other"}
        self.assertRaises(ToolContractError, lambda: resolve_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, tool_options, False))
        tool_options = {self.ALPHA:2.5}
        self.assertRaises(ToolContractError, lambda: resolve_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, tool_options, False))
        tool_options = {self.ALPHA:"abcdef"}
        self.assertRaises(ToolContractError, lambda: resolve_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, tool_options, False))
        tool_options = {self.BETA:"asdf"}
        self.assertRaises(ToolContractError, lambda: resolve_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, tool_options, False))
        tool_options = {self.GAMMA:1.0}
        self.assertRaises(ToolContractError, lambda: resolve_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, tool_options, False))
        tool_options = {self.GAMMA:""}
        self.assertRaises(ToolContractError, lambda: resolve_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, tool_options, False))
