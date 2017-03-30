import unittest
import tempfile
import logging
import uuid
import copy

from pbcommand.models import FileTypes, DataStoreFile

log = logging.getLogger(__name__)


class TestLoadFileTypes(unittest.TestCase):

    def test_file_types(self):
        # smoke test for loading file types
        ft = FileTypes.DS_ALIGN
        self.assertIsNotNone(ft)
        self.assertEqual(ft, copy.deepcopy(FileTypes.DS_ALIGN))
        self.assertNotEqual(ft, FileTypes.DS_ALIGN_CCS)

    def test_is_valid(self):
        ft = FileTypes.DS_ALIGN
        self.assertTrue(FileTypes.is_valid_id(ft.file_type_id))


class TestDataStore(unittest.TestCase):

    def test_datastore_file(self):
        tmpfile = tempfile.NamedTemporaryFile(suffix=".subreadset.xml").name
        ds = DataStoreFile(str(uuid.uuid4()), "pbcommand.tasks.dev_task", FileTypes.DS_SUBREADS.file_type_id, tmpfile, False, "Subreads", "Subread DataSet XML")
        log.info("DataStoreFile: {s}".format(s=ds))
        ds2 = DataStoreFile.from_dict(ds.to_dict())
        for attr in ["uuid", "file_type_id", "file_id", "path", "is_chunked", "name", "description"]:
            self.assertEqual(getattr(ds2, attr), getattr(ds, attr))
