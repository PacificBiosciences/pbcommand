import json
import os
import logging
import unittest
from pbcommand.models import ToolContract, ResolvedToolContract

from pbcommand.pb_io import (load_tool_contract_from,
                             load_resolved_tool_contract_from)
from pbcommand.schemas import validate_rtc, validate_tc
from pbcommand.utils import walker

from base_utils import DATA_DIR_RTC, DATA_DIR_TC


log = logging.getLogger(__name__)


def _to_json(path):
    with open(path, 'r') as f:
        d = json.loads(f.read())
    return d


def json_filter(path):
    return path.endswith(".json")


def _to_assertion(path, schema_validate_func):
    def test_is_validate(self):
        d = _to_json(path)
        # log.debug(d)
        log.info("Attempting to validate '{}'".format(path))
        is_valid = schema_validate_func(d)
        log.info(" is-valid? {i} {p}".format(i=is_valid, p=path))
        self.assertTrue(is_valid, "{p} is not valid with the avro schema".format(p=path))
    return test_is_validate


class ValidateResolvedToolContracts(unittest.TestCase):
    def test_validate_resolved_tool_contracts(self):
        for path in walker(DATA_DIR_RTC, json_filter):
            f = _to_assertion(path, validate_rtc)
            f(self)
            self.assertIsInstance(load_resolved_tool_contract_from(path), ResolvedToolContract)


class ValidateToolContracts(unittest.TestCase):
    def test_validate_tool_contracts(self):
        for path in walker(DATA_DIR_TC, json_filter):
            f = _to_assertion(path, validate_tc)
            f(self)
            self.assertIsInstance(load_tool_contract_from(path), ToolContract)