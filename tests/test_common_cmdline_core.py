import unittest
import logging
import shlex

from pbcommand.models import TaskTypes, get_default_contract_parser
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

    def test_parser_types(self):
        p = get_default_contract_parser(
            "pbcommand.tasks.test_common_cmdline_core",
            "0.1",
            "docstring",
            "pbcommand",
            TaskTypes.LOCAL,
            1,
            ())
        p.add_int("pbcommand.task_options.n", "n", default=0, name="N",
            description="Integer option")
        p.add_float("pbcommand.task_options.f", "f", default=0.0, name="F",
            description="Float option")
        # XXX note that the 'default' value is not actually what the option is
        # set to by default - it simply signals that action=store_true
        p.add_boolean("pbcommand.task_options.loud", "loud", default=True,
            name="Verbose", description="Boolean option")
        opts = p.arg_parser.parser.parse_args(["--n", "250", "--f", "1.2345", "--loud"])
        self.assertEqual(opts.n, 250)
        self.assertEqual(opts.f, 1.2345)
        self.assertTrue(opts.loud)

    # TODO we should add a lot more tests for parser behavior
