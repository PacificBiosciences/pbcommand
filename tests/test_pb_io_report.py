import json
import logging
import os
from pprint import pformat

from base_utils import get_data_file_from_subdir
from pbcommand.pb_io import load_report_from_json
from pbcommand.models.report import Plot, PlotlyPlot

_SERIALIZED_JSON_DIR = 'example-reports'


log = logging.getLogger(__name__)


def _to_report(name):
    file_name = get_data_file_from_subdir(_SERIALIZED_JSON_DIR, name)
    log.info("loading json report from {f}".format(f=file_name))
    r = load_report_from_json(file_name)
    return r


class TestSerializationOverviewReport:

    @classmethod
    def setup_class(cls):
        name = 'overview.json'
        cls.report = _to_report(name)

    def test_id(self):
        assert self.report.id == "overview"

    def test_uuid(self):
        assert self.report.uuid == "196136c8-f6fd-11e5-b481-3c15c2cc8f88"

    def test_title(self):
        assert self.report.title == "Overview Report"

    def test_attributes(self):
        assert len(self.report.attributes) == 2


class TestSerializationAdapterReport:

    @classmethod
    def setup_class(cls):
        file_name = 'filter_reports_adapters.json'
        cls.report = _to_report(file_name)

    def test_id(self):
        assert self.report.id == 'adapter'

    def test_attributes(self):
        assert len(self.report.attributes) == 6

    def test_plotgroups(self):
        assert len(self.report.plotGroups) == 2

    def test_plots(self):
        assert len(self.report.plotGroups[0].plots) == 1
        assert len(self.report.plotGroups[1].plots) == 1
        assert self.report.plotGroups[0].plots[0].plotType == Plot.PLOT_TYPE
        assert self.report.plotGroups[1].plots[0].plotType == PlotlyPlot.PLOT_TYPE
