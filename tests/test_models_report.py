import json
import logging
import os.path
import re
from pprint import pformat

import avro
import pytest

from base_utils import get_data_file_from_subdir, DATA_DIR
from pbcommand.pb_io import load_report_from_json, load_report_from, load_report_spec_from_json
from pbcommand.models.report import (Report, Attribute, PlotGroup, Plot, Table,
                                     Column, PbReportError, format_metric)
from pbcommand.schemas import validate_report

_SERIALIZED_JSON_DIR = 'example-reports'


log = logging.getLogger(__name__)


def _to_report(name):
    file_name = get_data_file_from_subdir(_SERIALIZED_JSON_DIR, name)
    log.info("loading json report from {f}".format(f=file_name))
    r = load_report_from_json(file_name)
    return r


class TestReportModel:

    def test_from_simple_dict(self):
        r = Report.from_simple_dict("pbcommand_test", {"n_reads": 50},
                                    "pbcommand")
        json_dict = json.loads(r.to_json())
        assert json_dict['attributes'] == [
            {
                "id": "pbcommand_test.pbcommand_n_reads",
                "name": "n_reads",
                "value": 50
            },
        ]

    def test_report_null_ns(self):
        """Can't create a report without a namespace."""
        with pytest.raises(PbReportError):
            r = Report(None)

    def test_report_empty_ns(self):
        """Can't create a report with an empty namespace."""
        with pytest.raises(PbReportError):
            r = Report("")

    def test_duplicate_ids(self):
        """Can't add elements with the same id."""
        with pytest.raises(PbReportError):
            r = Report('redfang')
            r.add_attribute(Attribute('a', 'b'))
            r.add_attribute(Attribute('a', 'c'))

    def test_illegal_id(self):
        """Ids must be alphanumberic with underscores"""
        with pytest.raises(PbReportError):
            r = Report('redfang')
            r.add_attribute(Attribute('a b', 'b'))
            r.add_attribute(Attribute('a', 'c'))

    def test_empty_id(self):
        with pytest.raises(PbReportError):
            r = Report('')

    def test_uppercase_id(self):
        with pytest.raises(PbReportError):
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

        r2 = load_report_from(d)
        assert r.uuid == r2.uuid

        assert 'redfang' == d['id']
        assert 'redfang.a' == d['attributes'][0]['id']
        assert 'redfang.a2' == d['attributes'][1]['id']
        assert 'redfang.pgid' == d['plotGroups'][0]['id']
        assert 'redfang.pgid.pid' == d['plotGroups'][0]['plots'][0]['id']
        assert 'redfang.pgid.pid2' == d['plotGroups'][0]['plots'][1]['id']

        assert 'redfang.tabid' == d['tables'][0]['id']
        assert 'redfang.tabid.c1' == d['tables'][0]['columns'][0]['id']

    def test_version_and_changelist(self):
        r = Report('example')
        d = r.to_dict()
        log.info("\n" + pformat(d))

        fields = ('version', 'uuid', 'plotGroups', 'tables', 'dataset_uuids')
        for field in fields:
            assert field in d

    def test_load_from_file(self):
        rpt_id = 'test_report'
        name = "test_report.json"
        path = get_data_file_from_subdir(_SERIALIZED_JSON_DIR, name)

        r = load_report_from(path)
        assert r.id == rpt_id

        r2 = load_report_from(r.to_dict())
        assert r2.id == rpt_id

    def test_to_dict_multi(self):
        """
        Multiple complex elements.
        The id of report sub elements is prepended with the id of the parent
        element when to_dict is called.
        """
        tags = ["alpha", "beta", "gamma"]
        r = Report('redfang', tags=tags)
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

        assert 'redfang' == d['id']
        assert 'redfang.a' == d['attributes'][0]['id']
        assert 'redfang.a2' == d['attributes'][1]['id']

        assert 'redfang.pgid' == d['plotGroups'][0]['id']
        assert 'redfang.pgid.pid' == d['plotGroups'][0]['plots'][0]['id']
        assert 'redfang.pgid.pid2' == d['plotGroups'][0]['plots'][1]['id']

        assert 'redfang.pgid2' == d['plotGroups'][1]['id']
        assert 'redfang.pgid2.pid2' == d['plotGroups'][1]['plots'][0]['id']
        assert 'redfang.pgid2.pid22' == d['plotGroups'][1]['plots'][1]['id']

        assert 'redfang.tabid' == d['tables'][0]['id']
        assert 'redfang.tabid.c1' == d['tables'][0]['columns'][0]['id']

        assert 'redfang.tabid2' == d['tables'][1]['id']
        assert 'redfang.tabid2.c2' == d['tables'][1]['columns'][0]['id']

        assert list(sorted(d['tags'])) == list(sorted(tags))

        loaded_report = load_report_from(d)
        assert list(sorted(loaded_report.tags)) == list(sorted(tags))

        log.info(repr(r))
        assert repr(r) is not None

    def test_get_attribute_by_id(self):
        a = Attribute('a', 'b')
        a2 = Attribute('b', 'b2')
        attributes = [a, a2]
        r = Report('redfang', attributes=attributes)

        a1 = r.get_attribute_by_id('a')

        assert a == a1

    def test_get_attribute_by_id_with_bad_id(self):
        a1 = Attribute('a', 'b')
        a2 = Attribute('b', 'b2')
        attributes = [a1, a2]
        report = Report('redfang', attributes=attributes)

        a = report.get_attribute_by_id('a')
        assert a.value == 'b'

        bad_a = report.get_attribute_by_id('id_that_does_not_exist')
        assert bad_a is None

    def test_get_table_by_id(self):
        r = Report('redfang')
        t1 = Table('tabid1')
        t1.add_column(Column('c1'))
        r.add_table(t1)

        t = r.get_table_by_id('tabid1')
        assert t == t1
        columns_d = t.to_columns_d()
        assert len(columns_d) == 0

    def test_get_table_by_id_with_bad_id(self):
        r = Report('redfang')
        t1 = Table('tabid1')
        t1.add_column(Column('c1'))
        r.add_table(t1)

        bad_t = r.get_table_by_id('id_that_does_not_exist')
        assert bad_t is None

    def test_get_column_by_id(self):
        r = Report('redfang')
        t1 = Table('tabid1')
        c1 = Column('c1')
        t1.add_column(c1)
        r.add_table(t1)

        c = r.get_table_by_id('tabid1').get_column_by_id('c1')
        assert c == c1

    def test_get_column_by_id_with_bad_id(self):
        r = Report('redfang')
        t1 = Table('tabid1')
        c1 = Column('c1')
        t1.add_column(c1)
        r.add_table(t1)

        bad_c = r.get_table_by_id('tabid1').get_column_by_id(
            'id_that_does_not_exist')
        assert bad_c is None

    def test_get_plotgroup_by_id(self):
        r = Report('redfang')
        pg1 = PlotGroup('pgid1')
        pg1.add_plot(Plot('pid1', 'anImg'))
        r.add_plotgroup(pg1)

        pg = r.get_plotgroup_by_id('pgid1')
        assert pg == pg1

    def test_get_plotgroup_by_id_with_bad_id(self):
        r = Report('redfang')
        pg1 = PlotGroup('pgid1')
        pg1.add_plot(Plot('pid1', 'anImg'))
        r.add_plotgroup(pg1)

        bad_pg = r.get_plotgroup_by_id('id_that_does_not_exist')
        assert bad_pg is None

    def test_get_plot_by_id(self):
        r = Report('redfang')
        pg1 = PlotGroup('pgid1')
        p1 = Plot('pid1', 'anImg')
        pg1.add_plot(p1)
        r.add_plotgroup(pg1)

        p = r.get_plotgroup_by_id('pgid1').get_plot_by_id('pid1')
        assert p == p1

    def test_get_plot_by_id_with_bad_id(self):
        r = Report('redfang')
        pg1 = PlotGroup('pgid1')
        p1 = Plot('pid1', 'anImg')
        pg1.add_plot(p1)
        r.add_plotgroup(pg1)

        bad_p = r.get_plotgroup_by_id(
            'pgid1').get_plot_by_id('id_that_does_not_exist')
        assert bad_p is None

    def test_merge(self):
        EXPECTED_VALUES = {
            "n_reads": 300,
            "n_zmws": 60,
            "sample": "Person1,Person2"
        }
        NAMES = {
            "n_reads": "Number of reads",
            "n_zmws": "Number of ZMWs",
            "sample": "Sample"
        }
        chunks = [
            Report("pbcommand_test",
                   attributes=[
                       Attribute(id_="n_reads", value=50,
                                 name="Number of reads"),
                       Attribute(id_="n_zmws", value=10, name="Number of ZMWs"),
                       Attribute(id_="sample", value="Person1", name="Sample")],
                   dataset_uuids=["12345"]),
            Report("pbcommand_test",
                   attributes=[
                       Attribute(id_="n_reads", value=250,
                                 name="Number of reads"),
                       Attribute(id_="n_zmws", value=50, name="Number of ZMWs"),
                       Attribute(id_="sample", value="Person2", name="Sample")]),
        ]
        r = Report.merge(chunks)
        assert [a.id for a in r.attributes] == ["n_reads", "n_zmws", "sample"]
        assert r._dataset_uuids == ["12345"]
        for attr in r.attributes:
            assert attr.value == EXPECTED_VALUES[attr.id]
            assert attr.name == NAMES[attr.id]
        for table in r.tables:
            for column in table.columns:
                assert column.header == NAMES[column.id]

    def test_merge_tables(self):
        names = ['laa_report1.json', 'laa_report2.json']
        r = Report.merge([_to_report(names[0]), _to_report(names[1])])
        table = r.tables[0]
        assert len(table.columns) == 7
        assert [col.header for col in table.columns] == [
            'BarcodeName', 'FastaName', 'CoarseCluster', 'Phase',
            'TotalCoverage', 'SequenceLength', 'PredictedAccuracy']
        for col in table.columns:
            assert len(col.values) == 4
            if col.header == 'BarcodeName':
                assert col.values == [
                    'Barcode1', 'Barcode2', 'Barcode4', 'Barcode3']
            elif col.header == 'FastaName':
                assert col.values == [
                    'BarcodeFasta1', 'BarcodeFasta2', 'BarcodeFasta4',
                    'BarcodeFasta3']
            else:
                assert col.values == [1, 2, 4, 3]

        column_list_d = table.to_columns_d()
        assert len(column_list_d) == 4


class TestMalformedReport:

    def test_bad_01(self):
        r = Report("stuff", uuid=1234)
        d = r.to_dict()

        def fx():
            # when the Report validation is enabled, use to_json
            # r.to_json()
            return validate_report(d)

        with pytest.raises(IOError):
            fx()


class TestReportSchemaVersion100:

    name = "example_version_1_0_0.json"

    def test_sanity(self):
        r = _to_report(self.name)
        assert isinstance(r, Report)


class TestRepotSchemaVersion100WithPlots(TestReportSchemaVersion100):
    name = "example_with_plot.json"


class TestReportSpec:

    def setup_method(self, method):
        self.spec = load_report_spec_from_json(
            os.path.join(DATA_DIR, "report-specs", "report_spec.json"))

    def test_report_validation(self):
        rpt = _to_report("test_report.json")
        r = self.spec.validate_report(rpt)
        assert isinstance(r, Report)
        rpt.attributes.append(Attribute("attribute5", value=12345))
        def error_len(e): return len(str(e).split("\n"))
        try:
            self.spec.validate_report(rpt)
        except ValueError as e:
            assert error_len(e) == 2
        else:
            self.fail("Expected exception")
        assert not self.spec.is_valid_report(rpt)
        rpt.attributes[0] = Attribute("attribute1", value=1.2345)
        try:
            self.spec.validate_report(rpt)
        except ValueError as e:
            print(e)
            assert error_len(e) == 3
        else:
            self.fail("Expected exception")
        assert not self.spec.is_valid_report(rpt)

    def test_format_metric(self):
        s = format_metric("{:,d}", 123456789)
        assert s == "123,456,789"
        s = format_metric("{:.4g}", 1.2345678)
        assert s == "1.235"
        s = format_metric("{M:.2f} Mb", 123456789)
        assert s == "123.46 Mb"
        s = format_metric("{p:.5g}%", 0.987654321)
        assert s == "98.765%"
        s = format_metric("{p:g}", 0.000001)
        assert s == "0.0001%"
        s = format_metric("{:,.3f}", 1000000.2345678)
        assert s == "1,000,000.235"

    def test_apply_view(self):
        rpt = _to_report("test_report2.json")
        rpt = self.spec.apply_view(rpt)
        assert all([a.name is not None for a in rpt.attributes])
        assert all([t.title is not None for t in rpt.tables])
        assert all([c.header is not None for c in rpt.tables[0].columns])
        assert all([pg.title is not None for pg in rpt.plotGroups])
        assert all([p.title is not None for p in rpt.plotGroups[0].plots])
