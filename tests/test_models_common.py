import unittest
import logging

from pbcommand.models import FileTypes

log = logging.getLogger(__name__)


class TestLoadFileTypes(unittest.TestCase):

    def test_file_types(self):
        # smoke test for loading file types
        ft = FileTypes.DS_ALIGN
        self.assertIsNotNone(ft)

    def test_is_valid(self):
        ft = FileTypes.DS_ALIGN
        self.assertTrue(FileTypes.is_valid_id(ft.file_type_id))
