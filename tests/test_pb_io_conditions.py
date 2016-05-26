import unittest
import logging

from base_utils import get_data_file_from_subdir

from pbcommand.pb_io import load_reseq_conditions_from


log = logging.getLogger(__name__)


_SERIALIZED_JSON_DIR = 'example-conditions'


def _loader(name):
    file_name = get_data_file_from_subdir(_SERIALIZED_JSON_DIR, name)
    log.info("loading json report from {f}".format(f=file_name))
    r = load_reseq_conditions_from(file_name)
    return r


class TestSerializationOfResequencingConditions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        name = 'reseq-conditions-01.json'
        cls.cs = _loader(name)

    def test_condition_n(self):
        self.assertEqual(len(self.cs.conditions), 3)

    def test_condition_a(self):
        log.info(self.cs)
        self.assertEqual(self.cs.conditions[0].cond_id, "cond_alpha")
