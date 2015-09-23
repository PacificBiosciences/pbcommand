
import unittest
import json
import logging

from pbcommand.models.report import Report
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
        r = Report.merge([
            Report.from_simple_dict("pbcommand_test",
                                    {"n_reads": 50, "n_zmws": 10},
                                    "pbcommand"),
            Report.from_simple_dict("pbcommand_test",
                                    {"n_reads": 250, "n_zmws": 50},
                                    "pbcommand")])
        attr = {a.id: a.value for a in r.attributes}
        self.assertEqual(attr['pbcommand_n_reads'], 300)
        self.assertEqual(attr['pbcommand_n_zmws'], 60)

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
