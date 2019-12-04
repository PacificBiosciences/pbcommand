import logging
import os
import subprocess
import tempfile

from .base_utils import (HAS_PBCORE,
                         pbcore_skip_msg,
                         get_temp_file,
                         get_temp_dir)

log = logging.getLogger(__name__)


class PbIntegrationBase:

    def setup_method(self, method):
        self._cwd = os.getcwd()
        self._tmp_dir = tempfile.mkdtemp()
        os.chdir(self._tmp_dir)

    def teardown_method(self, method):
        os.chdir(self._cwd)

    def _check_call(self, args):
        log.info("Writing logs to subprocess.std* in %s", self._tmp_dir)
        with open("subprocess.stdout", "w") as stdout:
            with open("subprocess.stderr", "w") as stderr:
                try:
                    return subprocess.check_call(
                        args, stdout=stdout, stderr=stderr)
                except Exception as e:
                    log.error(e)
                    log.error("Console outputs are in %s", self._tmp_dir)
                    raise
