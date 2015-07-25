import os
import logging
import unittest
import json
from pprint import pformat

from pbcommand.pb_io import load_report_from_json

_SERIALIZED_JSON_DIR = 'example-reports'

from base_utils import get_data_file_from_subdir

log = logging.getLogger(__name__)


def _to_report(name):
    file_name = get_data_file_from_subdir(_SERIALIZED_JSON_DIR, name)
    log.info("loading json report from {f}".format(f=file_name))
    r = load_report_from_json(file_name)
    return r


class TestSerializationOverviewReport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        name = 'overview.json'
        cls.report = _to_report(name)

    def test_id(self):
        self.assertEqual(self.report.id, "overview")

    def test_attributes(self):
        self.assertTrue(len(self.report.attributes), 2)


class TestSerializationAdapterReport(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        file_name = 'filter_reports_adapters.json'
        cls.report = _to_report(file_name)

    def test_id(self):
        self.assertEqual(self.report.id, 'adapter')

    def test_attributes(self):
        self.assertEqual(len(self.report.attributes), 6)

    def test_plotgroups(self):
        self.assertEqual(len(self.report.plotGroups), 1)

    def test_plots(self):
        self.assertEqual(len(self.report.plotGroups[0].plots), 1)
