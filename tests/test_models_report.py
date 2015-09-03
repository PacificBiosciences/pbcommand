
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


if __name__ == "__main__":
    unittest.main()
