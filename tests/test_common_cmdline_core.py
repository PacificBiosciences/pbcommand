import json
import logging
import os.path
import shlex
import tempfile

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
    p = CU.add_log_debug_option(p)
    p.add_argument('example_file', type=str, help="No testing of existence")
    return p


def _example_main(cmdline_args):
    """Example func for testing."""
    p = _example_parser()
    argv = shlex.split(cmdline_args)
    rcode = pacbio_args_runner(argv, p, args_runner, log, setup_log)
    return rcode


def args_runner_fail(*args, **kwargs):
    raise RuntimeError("Oops, something crashed")


def _example_main_fail(cmdline_args):
    """Example func for testing."""
    p = _example_parser()
    argv = shlex.split(cmdline_args)
    rcode = pacbio_args_runner(argv, p, args_runner_fail, log, setup_log)
    return rcode


class SimpleTest:

    def setup_method(self, method):
        tmpdir = tempfile.mkdtemp(suffix="cromwell-executions")
        self._cwd = os.getcwd()
        os.chdir(tmpdir)

    def teardown_method(self, method):
        os.chdir(self._cwd)

    def test_01(self):
        args = "--debug /path/to/my_fake_file.txt"
        rcode = _example_main(args)
        assert rcode == 0
        assert not os.path.isfile("alarms.json")

    def test_dump_alarm_on_error(self):
        args = "--debug /path/to/my_fake_file.txt"
        os.environ["SMRT_PIPELINE_BUNDLE_DIR"] = "true"
        rcode = _example_main_fail(args)
        assert rcode == 2
        assert os.path.isfile("alarms.json")
        with open("alarms.json", "r") as json_in:
            d = json.loads(json_in.read())[0]
            assert d["severity"] == "ERROR"
        with open("task-report.json", "r") as rpt_in:
            d = json.loads(rpt_in.read())
            a = {a["id"]: a["value"] for a in d["attributes"]}
            assert a["workflow_task.exit_code"] == 2
            assert a["workflow_task.nproc"] == 1
