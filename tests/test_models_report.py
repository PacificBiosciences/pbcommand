
import unittest
import json

from pbcommand.models.report import Report


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


if __name__ == "__main__":
    unittest.main()
