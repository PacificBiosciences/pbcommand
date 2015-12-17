
import unittest
import json
import logging

from pbcommand.models.report import Report, Attribute
from pbcommand.pb_io import load_report_from_json

_SERIALIZED_JSON_DIR = 'example-reports'

from base_utils import get_data_file_from_subdir

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
                    Attribute(id_="n_reads", value=50, name="Number of reads"),
                    Attribute(id_="n_zmws", value=10, name="Number of ZMWs")]),
            Report("pbcommand_test",
                attributes=[
                    Attribute(id_="n_reads", value=250, name="Number of reads"),
                    Attribute(id_="n_zmws", value=50, name="Number of ZMWs")]),
        ]
        r = Report.merge(chunks)
        self.assertEqual([a.id for a in r.attributes], ["n_reads", "n_zmws"])
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


if __name__ == "__main__":
    unittest.main()
