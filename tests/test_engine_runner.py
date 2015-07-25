import logging
import unittest

from pbcommand.engine import run_cmd

from .base_utils import get_temp_file, get_temp_dir

log = logging.getLogger(__name__)


class RunnerSmokeTest(unittest.TestCase):

    def test_simple_run_cmd(self):
        d = get_temp_dir("simple-cmd")
        txt_in = get_temp_file(".txt", d)
        txt_out = get_temp_file("*.txt", d)
        exe = "cat {i} > {o}".format(i=txt_in, o=txt_out)

        # this could all be bundled into a context manager
        # with RunCommand('/path/stdout', '/path/to/stderr') as r:
        #   r.exe("echo 'exe1')
        #   r.exe("echo 'exe2')
        #   result = r.get_result() # close the file handles
        stdout = get_temp_file("-stdout", d)
        stderr = get_temp_file("-stderr", d)
        with open(stdout, 'w') as fo:
            with open(stderr, 'w') as fe:
                result = run_cmd(exe, fo, fe)

        emgs = "Command {e} failed".format(e=exe)
        self.assertEquals(result.exit_code, 0, emgs)
