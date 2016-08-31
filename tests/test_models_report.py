import json
import logging
from pprint import pformat
import os.path
import re
import unittest

from pbcommand.pb_io import load_report_from_json, load_report_spec_from_json
from pbcommand.models.report import (Report, Attribute, PlotGroup, Plot, Table,
                                     Column, PbReportError, format_metric)
from pbcommand.schemas import validate_report

_SERIALIZED_JSON_DIR = 'example-reports'

from base_utils import get_data_file_from_subdir, DATA_DIR

log = logging.getLogger(__name__)


def _to_report(name):
    file_name = get_data_file_from_subdir(_SERIALIZED_JSON_DIR, name)
    log.info("loading json report from {f}".format(f=file_name))
    r = load_report_from_json(file_name)
    return r


class TestReportModel(unittest.TestCase):

    def test_from_simple_dict(self):
        r = Report.from_simple_dict("pbcommand_test", {"n_reads": 50},
                                    "pbcommand")
        json_dict = json.loads(r.to_json())
        self.assertEqual(json_dict['attributes'], [
            {
                "id": "pbcommand_test.pbcommand_n_reads",
                "name": "n_reads",
                "value": 50
            },
        ])

    def test_report_null_ns(self):
        """Can't create a report without a namespace."""
        with self.assertRaises(PbReportError):
            r = Report(None)

    def test_report_empty_ns(self):
        """Can't create a report with an empty namespace."""
        with self.assertRaises(PbReportError):
            r = Report("")

    def test_duplicate_ids(self):
        """Can't add elements with the same id."""
        with self.assertRaises(PbReportError):
            r = Report('redfang')
            r.add_attribute(Attribute('a', 'b'))
            r.add_attribute(Attribute('a', 'c'))

    def test_illegal_id(self):
        """Ids must be alphanumberic with underscores"""
        with self.assertRaises(PbReportError):
            r = Report('redfang')
            r.add_attribute(Attribute('a b', 'b'))
            r.add_attribute(Attribute('a', 'c'))

    def test_empty_id(self):
        with self.assertRaises(PbReportError):
            r = Report('')

    def test_uppercase_id(self):
        with self.assertRaises(PbReportError):
            r = Report('A')

    def test_to_dict(self):
        """
        The id of report sub elements is prepended with the id of the parent
        element when to_dict is called.
        """
        r = Report('redfang')
        a = Attribute('a', 'b')
        a2 = Attribute('a2', 'b2')
        r.add_attribute(a)
        r.add_attribute(a2)

        pg = PlotGroup('pgid')
        pg.add_plot(Plot('pid', 'anImg'))
        pg.add_plot(Plot('pid2', 'anImg2'))
        r.add_plotgroup(pg)

        t = Table('tabid')
        t.add_column(Column('c1'))
        r.add_table(t)

        d = r.to_dict()

        log.debug("\n" + pformat(d))

        self.assertEqual('redfang', d['id'])
        self.assertEqual('redfang.a', d['attributes'][0]['id'])
        self.assertEqual('redfang.a2', d['attributes'][1]['id'])
        self.assertEqual('redfang.pgid', d['plotGroups'][0]['id'])
        self.assertEqual('redfang.pgid.pid', d[
                         'plotGroups'][0]['plots'][0]['id'])
        self.assertEqual('redfang.pgid.pid2', d[
                         'plotGroups'][0]['plots'][1]['id'])

        self.assertEqual('redfang.tabid', d['tables'][0]['id'])
        self.assertEqual('redfang.tabid.c1', d['tables'][
                         0]['columns'][0]['id'])

    def test_version_and_changelist(self):
        r = Report('example')
        d = r.to_dict()
        log.info("\n" + pformat(d))

        fields = ('version', 'uuid', 'plotGroups', 'tables', 'dataset_uuids')
        for field in fields:
            self.assertTrue(field in d)

    def test_to_dict_multi(self):
        """
        Multiple complex elements.
        The id of report sub elements is prepended with the id of the parent
        element when to_dict is called.
        """
        r = Report('redfang')
        a = Attribute('a', 'b')
        a2 = Attribute('a2', 'b2')
        r.add_attribute(a)
        r.add_attribute(a2)

        pg = PlotGroup('pgid')
        pg.add_plot(Plot('pid', 'anImg'))
        pg.add_plot(Plot('pid2', 'anImg2'))
        r.add_plotgroup(pg)

        pg = PlotGroup('pgid2')
        pg.add_plot(Plot('pid2', 'anImg2'))
        pg.add_plot(Plot('pid22', 'anImg22'))
        r.add_plotgroup(pg)

        t = Table('tabid')
        t.add_column(Column('c1'))
        r.add_table(t)

        t = Table('tabid2')
        t.add_column(Column('c2'))
        r.add_table(t)

        d = r.to_dict()

        log.debug(str(d))

        self.assertEqual('redfang', d['id'])
        self.assertEqual('redfang.a', d['attributes'][0]['id'])
        self.assertEqual('redfang.a2', d['attributes'][1]['id'])

        self.assertEqual('redfang.pgid', d['plotGroups'][0]['id'])
        self.assertEqual('redfang.pgid.pid', d[
                         'plotGroups'][0]['plots'][0]['id'])
        self.assertEqual('redfang.pgid.pid2', d[
                         'plotGroups'][0]['plots'][1]['id'])

        self.assertEqual('redfang.pgid2', d['plotGroups'][1]['id'])
        self.assertEqual('redfang.pgid2.pid2', d[
                         'plotGroups'][1]['plots'][0]['id'])
        self.assertEqual('redfang.pgid2.pid22', d[
                         'plotGroups'][1]['plots'][1]['id'])

        self.assertEqual('redfang.tabid', d['tables'][0]['id'])
        self.assertEqual('redfang.tabid.c1', d['tables'][
                         0]['columns'][0]['id'])

        self.assertEqual('redfang.tabid2', d['tables'][1]['id'])
        self.assertEqual('redfang.tabid2.c2', d[
                         'tables'][1]['columns'][0]['id'])

        log.info(repr(r))
        self.assertIsNotNone(repr(r))

    def test_get_attribute_by_id(self):
        a = Attribute('a', 'b')
        a2 = Attribute('b', 'b2')
        attributes = [a, a2]
        r = Report('redfang', attributes=attributes)

        a1 = r.get_attribute_by_id('a')

        self.assertEqual(a, a1)

    def test_get_attribute_by_id_with_bad_id(self):
        a1 = Attribute('a', 'b')
        a2 = Attribute('b', 'b2')
        attributes = [a1, a2]
        report = Report('redfang', attributes=attributes)

        a = report.get_attribute_by_id('a')
        self.assertEqual(a.value, 'b')

        bad_a = report.get_attribute_by_id('id_that_does_not_exist')
        self.assertIsNone(bad_a)

    def test_get_table_by_id(self):
        r = Report('redfang')
        t1 = Table('tabid1')
        t1.add_column(Column('c1'))
        r.add_table(t1)

        t = r.get_table_by_id('tabid1')
        self.assertEqual(t, t1)

    def test_get_table_by_id_with_bad_id(self):
        r = Report('redfang')
        t1 = Table('tabid1')
        t1.add_column(Column('c1'))
        r.add_table(t1)

        bad_t = r.get_table_by_id('id_that_does_not_exist')
        self.assertIsNone(bad_t)

    def test_get_column_by_id(self):
        r = Report('redfang')
        t1 = Table('tabid1')
        c1 = Column('c1')
        t1.add_column(c1)
        r.add_table(t1)

        c = r.get_table_by_id('tabid1').get_column_by_id('c1')
        self.assertEqual(c, c1)

    def test_get_column_by_id_with_bad_id(self):
        r = Report('redfang')
        t1 = Table('tabid1')
        c1 = Column('c1')
        t1.add_column(c1)
        r.add_table(t1)

        bad_c = r.get_table_by_id('tabid1').get_column_by_id(
            'id_that_does_not_exist')
        self.assertIsNone(bad_c)

    def test_get_plotgroup_by_id(self):
        r = Report('redfang')
        pg1 = PlotGroup('pgid1')
        pg1.add_plot(Plot('pid1', 'anImg'))
        r.add_plotgroup(pg1)

        pg = r.get_plotgroup_by_id('pgid1')
        self.assertEqual(pg, pg1)

    def test_get_plotgroup_by_id_with_bad_id(self):
        r = Report('redfang')
        pg1 = PlotGroup('pgid1')
        pg1.add_plot(Plot('pid1', 'anImg'))
        r.add_plotgroup(pg1)

        bad_pg = r.get_plotgroup_by_id('id_that_does_not_exist')
        self.assertIsNone(bad_pg)

    def test_get_plot_by_id(self):
        r = Report('redfang')
        pg1 = PlotGroup('pgid1')
        p1 = Plot('pid1', 'anImg')
        pg1.add_plot(p1)
        r.add_plotgroup(pg1)

        p = r.get_plotgroup_by_id('pgid1').get_plot_by_id('pid1')
        self.assertEqual(p, p1)

    def test_get_plot_by_id_with_bad_id(self):
        r = Report('redfang')
        pg1 = PlotGroup('pgid1')
        p1 = Plot('pid1', 'anImg')
        pg1.add_plot(p1)
        r.add_plotgroup(pg1)

        bad_p = r.get_plotgroup_by_id(
            'pgid1').get_plot_by_id('id_that_does_not_exist')
        self.assertIsNone(bad_p)

    def test_merge(self):
        EXPECTED_VALUES = {
            "n_reads": 300,
            "n_zmws": 60,
        }
        NAMES = {
            "n_reads": "Number of reads",
            "n_zmws": "Number of ZMWs"
        }
        chunks = [
            Report("pbcommand_test",
                   attributes=[
                       Attribute(id_="n_reads", value=50,
                                 name="Number of reads"),
                       Attribute(id_="n_zmws", value=10, name="Number of ZMWs")],
                   dataset_uuids=["12345"]),
            Report("pbcommand_test",
                   attributes=[
                       Attribute(id_="n_reads", value=250,
                                 name="Number of reads"),
                       Attribute(id_="n_zmws", value=50, name="Number of ZMWs")]),
        ]
        r = Report.merge(chunks)
        self.assertEqual([a.id for a in r.attributes], ["n_reads", "n_zmws"])
        self.assertEqual(r._dataset_uuids, ["12345"])
        for attr in r.attributes:
            self.assertEqual(attr.value, EXPECTED_VALUES[attr.id])
            self.assertEqual(attr.name, NAMES[attr.id])
        for table in r.tables:
            for column in table.columns:
                self.assertEqual(column.header, NAMES[column.id])

    def test_merge_tables(self):
        names = ['laa_report1.json', 'laa_report2.json']
        r = Report.merge([_to_report(names[0]), _to_report(names[1])])
        table = r.tables[0]
        self.assertEqual(len(table.columns), 7)
        self.assertEqual(
            [col.header for col in table.columns],
            ['BarcodeName', 'FastaName', 'CoarseCluster', 'Phase',
             'TotalCoverage', 'SequenceLength', 'PredictedAccuracy'])
        for col in table.columns:
            self.assertEqual(len(col.values), 4)
            if col.header == 'BarcodeName':
                self.assertEqual(
                    col.values,
                    ['Barcode1', 'Barcode2', 'Barcode4', 'Barcode3'])
            elif col.header == 'FastaName':
                self.assertEqual(
                    col.values,
                    ['BarcodeFasta1', 'BarcodeFasta2', 'BarcodeFasta4',
                     'BarcodeFasta3'])
            else:
                self.assertEqual(col.values, [1, 2, 4, 3])


class TestMalformedReport(unittest.TestCase):

    def test_bad_01(self):
        r = Report("stuff", uuid=1234)
        d = r.to_dict()

        def fx():
            # when the Report validation is enabled, use to_json
            # r.to_json()
            return validate_report(d)

        self.assertRaises(IOError, fx)


class TestReportSchemaVersion100(unittest.TestCase):

    name = "example_version_1_0_0.json"

    def test_sanity(self):
        r = _to_report(self.name)
        self.assertIsInstance(r, Report)


class TestRepotSchemaVersion100WithPlots(TestReportSchemaVersion100):
    name = "example_with_plot.json"


class TestReportSpec(unittest.TestCase):

    def setUp(self):
        self.spec = load_report_spec_from_json(
            os.path.join(DATA_DIR, "report-specs", "report_spec.json"))

    def test_report_validation(self):
        rpt = _to_report("test_report.json")
        errors = self.spec.validate_report(rpt, False)
        self.assertEqual(len(errors), 0)
        rpt.attributes.append(Attribute("attribute5", value=12345))
        errors = self.spec.validate_report(rpt, False)
        self.assertEqual(len(errors), 1)
        rpt.attributes[0] = Attribute("attribute1", value=1.2345)
        errors = self.spec.validate_report(rpt, False)
        self.assertEqual(len(errors), 2)

    def test_format_metric(self):
        s = format_metric("{:,d}", 123456789)
        self.assertEqual(s, "123,456,789")
        s = format_metric("{:.4g}", 1.2345678)
        self.assertEqual(s, "1.235")
        s = format_metric("{M:.2f} Mb", 123456789)
        self.assertEqual(s, "123.46 Mb")
        s = format_metric("{p:.5g}%", 0.987654321)
        self.assertEqual(s, "98.765%")
        s = format_metric("{p:g}", 0.000001)
        self.assertEqual(s, "0.0001%")
