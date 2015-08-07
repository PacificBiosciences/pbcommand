import os
import logging
import unittest

from pbcommand.pb_io import load_tool_contract_from, load_resolved_tool_contract_from
from pbcommand.schemas import validate_rtc, validate_tc

from base_utils import DATA_DIR

log = logging.getLogger(__name__)


def _get_all_rtcs(root_dir):
    for path in os.listdir(root_dir):
        if path.endswith('resolved_tool_contract.json'):
            yield os.path.join(root_dir, path)


def _to_assertion(path):
    def test_is_validate(self):
        rtc = load_resolved_tool_contract_from(path)
        log.debug(rtc)
        is_valid = validate_rtc(rtc.to_dict())
        self.assertTrue(is_valid, "RTC {p} is not valid".format(p=path))
    return test_is_validate


class ValidateResolvedToolContracts(unittest.TestCase):
    def test_validate_resolved_tool_contracts(self):
        for path in _get_all_rtcs(DATA_DIR):
            f = _to_assertion(path)
            f(self)
