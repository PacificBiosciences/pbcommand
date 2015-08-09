import logging
import unittest

from base_utils import get_data_file, get_temp_dir
from pbcommand.models import ResolvedToolContract, ResolvedScatteredToolContractTask, ResolvedGatherToolContractTask

from pbcommand.pb_io import load_tool_contract_from
from pbcommand.resolver import resolve_scatter_tool_contract, resolve_gather_tool_contract

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
        tc = load_tool_contract_from(get_data_file(self.FILE_NAME))
        rtc = resolve_scatter_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, self.TOOL_OPTIONS, self.MAX_NCHUNKS, self.CHUNK_KEYS)
        self.assertIsInstance(rtc, ResolvedToolContract)
        self.assertIsInstance(rtc.task, ResolvedScatteredToolContractTask)
        self.assertEqual(rtc.task.max_nchunks, 7)


class TestGatherResolver(unittest.TestCase):
    FILE_NAME = "dev_gather_fasta_app_tool_contract.json"
    MAX_NCHUNKS = 7
    MAX_NPROC = 9
    INPUT_FILES = ['/tmp/file.fasta.chunk.json']
    CHUNK_KEY = '$chunk.filter_fasta_id'

    TOOL_OPTIONS = {}

    def test_sanity(self):
        d = get_temp_dir("resolved-tool-contract")
        tc = load_tool_contract_from(get_data_file(self.FILE_NAME))
        rtc = resolve_gather_tool_contract(tc, self.INPUT_FILES, d, d, self.MAX_NPROC, self.TOOL_OPTIONS, self.CHUNK_KEY)
        self.assertIsInstance(rtc, ResolvedToolContract)
        self.assertIsInstance(rtc.task, ResolvedGatherToolContractTask)
        self.assertEqual(rtc.task.chunk_key, self.CHUNK_KEY)
