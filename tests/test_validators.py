import traceback
import tempfile
import unittest
import logging
import os.path as op
import os

from pbcommand.validators import *

from base_utils import get_data_file_from_subdir

log = logging.getLogger(__name__)


def _to_report(name):
    return get_data_file_from_subdir("example-reports", name)


class TestValidators(unittest.TestCase):

    def test_validate_file(self):
        """
        Test: Return abspath if file found
        """
        file_path = validate_file(op.relpath(__file__, "."))
        self.assertEqual(file_path, __file__)

    def test_validate_file_fails(self):
        """
        Test: Raise IOError if file not found
        """
        with self.assertRaises(IOError):
            validate_file("this_file_does_not_exist")

    def test_validate_nonempty_file(self):
        """
        Test: Return abspath if file found and not empty
        """
        file_path = validate_nonempty_file(op.relpath(__file__, os.getcwd()))
        self.assertEqual(file_path, __file__)

    def test_validate_nonempty_file_fails(self):
        """
        Test: Raise IOError if file found but empty
        """
        f = tempfile.NamedTemporaryFile(suffix="empty").name
        with self.assertRaises(IOError):
            validate_nonempty_file(f)

    def test_validate_output_dir(self):
        """
        Test: Raise IOError if output does not exist
        """
        try:
            validate_output_dir(op.dirname(__file__))
        except:
            log.error(traceback.format_exc())
            raise Exception("Directory validation failed")
        with self.assertRaises(IOError):
            validate_output_dir('/whatev')

    def test_validate_report_file(self):
        """
        Test: Raise ValueError if report has path sep
        """
        try:
            # we know this gets made
            validate_report_file('foo')
        except:
            log.error(traceback.format_exc())
            raise Exception("Filename validation failed")
        with self.assertRaises(ValueError):
            validate_report_file('/foo')

    def test_validate_report(self):
        rpt = _to_report("test_report.json")
        rpt2 = validate_report(rpt)

    def test_validate_report_fails(self):
        rpt = _to_report("test_report2.json")
        with self.assertRaises(ValueError):
            validate_report(rpt)
