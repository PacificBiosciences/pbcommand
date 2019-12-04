import logging
import os
import os.path as op
import tempfile
import traceback

import pytest

from base_utils import get_data_file_from_subdir
from pbcommand.validators import *

log = logging.getLogger(__name__)


def _to_report(name):
    return get_data_file_from_subdir("example-reports", name)


class TestValidators:

    def test_validate_file(self):
        """
        Test: Return abspath if file found
        """
        file_path = validate_file(op.relpath(__file__, "."))
        assert file_path == __file__

    def test_validate_file_fails(self):
        """
        Test: Raise IOError if file not found
        """
        with pytest.raises(IOError):
            validate_file("this_file_does_not_exist")

    def test_validate_nonempty_file(self):
        """
        Test: Return abspath if file found and not empty
        """
        file_path = validate_nonempty_file(op.relpath(__file__, os.getcwd()))
        assert file_path == __file__

    def test_validate_nonempty_file_fails(self):
        """
        Test: Raise IOError if file found but empty
        """
        f = tempfile.NamedTemporaryFile(suffix="empty").name
        with pytest.raises(IOError):
            validate_nonempty_file(f)

    def test_validate_output_dir(self):
        """
        Test: Raise IOError if output does not exist
        """
        try:
            validate_output_dir(op.dirname(__file__))
        except BaseException:
            log.error(traceback.format_exc())
            raise Exception("Directory validation failed")
        with pytest.raises(IOError):
            validate_output_dir('/whatev')

    def test_validate_report_file(self):
        """
        Test: Raise ValueError if report has path sep
        """
        try:
            # we know this gets made
            validate_report_file('foo')
        except BaseException:
            log.error(traceback.format_exc())
            raise Exception("Filename validation failed")
        with pytest.raises(ValueError):
            validate_report_file('/foo')

    def test_validate_report(self):
        rpt = _to_report("test_report.json")
        rpt2 = validate_report(rpt)

    def test_validate_report_fails(self):
        rpt = _to_report("test_report2.json")
        with pytest.raises(ValueError):
            validate_report(rpt)
