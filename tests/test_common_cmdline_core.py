import unittest
import logging
import shlex

import pbcommand.common_options as CU
from pbcommand.cli.core import pacbio_args_runner
from pbcommand.cli import get_default_argparser
from pbcommand.utils import setup_log

log = logging.getLogger(__name__)


def args_runner(*args, **kwargs):
    log.info("Running args: {a}".format(a=args))
    return 0


def _example_parser():
    p = get_default_argparser("1.0.0", "Example Mock Parser")
    p = CU.add_debug_option(p)
    p.add_argument('example_file', type=str, help="No testing of existence")
    return p


def _example_main(cmdline_args):
    """Example func for testing."""
    p = _example_parser()
    argv = shlex.split(cmdline_args)
    rcode = pacbio_args_runner(argv, p, args_runner, log, setup_log)
    return rcode


class SimpleTest(unittest.TestCase):

    def test_01(self):
        args = "--debug /path/to/my_fake_file.txt"
        rcode = _example_main(args)
        self.assertEqual(rcode, 0)
