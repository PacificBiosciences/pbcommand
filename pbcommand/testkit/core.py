
import os
import unittest
import logging
import tempfile
import subprocess

from .base_utils import (HAS_PBCORE,
                         pbcore_skip_msg,
                         get_temp_file,
                         get_temp_dir)

log = logging.getLogger(__name__)


class PbIntegrationBase(unittest.TestCase):

    def setUp(self):
        self._cwd = os.getcwd()
        self._tmp_dir = tempfile.mkdtemp()
        os.chdir(self._tmp_dir)

    def tearDown(self):
        os.chdir(self._cwd)

    def _check_call(self, args):
        log.info("Writing logs to subprocess.std* in %s", self._tmp_dir)
        with open("subprocess.stdout", "w") as stdout:
            with open("subprocess.stderr", "w") as stderr:
                try:
                    return subprocess.check_call(args, stdout=stdout, stderr=stderr)
                except Exception as e:
                    log.error(e)
                    log.error("Console outputs are in %s", self._tmp_dir)
                    self.fail("Subprocess call failed: %s" % e)
