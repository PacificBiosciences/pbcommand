from __future__ import absolute_import
import logging

from base_utils import get_data_file
from pbcommand.testkit import PbTestApp
from pbcommand.resolver import ToolContractError

log = logging.getLogger(__name__)


class TestRunDevApp(PbTestApp):
    DRIVER_BASE = "python -m pbcommand.cli.examples.dev_app "
    REQUIRES_PBCORE = True
    INPUT_FILES = [get_data_file("example.fasta")]
    TASK_OPTIONS = {"pbcommand.task_options.dev_read_length": 27}


class TestTxtDevApp(PbTestApp):
    DRIVER_BASE = "python -m pbcommand.cli.examples.dev_txt_app "
    # XXX using default args, so the emit/resolve drivers are automatic
    REQUIRES_PBCORE = False
    INPUT_FILES = [get_data_file("example.txt")]
    TASK_OPTIONS = {"pbcommand.task_options.dev_max_nlines": 27}
    RESOLVED_TASK_OPTIONS = {"pbcommand.task_options.dev_max_nlines": 27}


class TestQuickDevHelloWorld(PbTestApp):
    """Runs dev_qhello_world """
    DRIVER_EMIT = "python -m pbcommand.cli.examples.dev_quick_hello_world  emit-tool-contract pbcommand.tasks.dev_qhello_world "
    DRIVER_RESOLVE = "python -m pbcommand.cli.examples.dev_quick_hello_world  run-rtc "

    REQUIRES_PBCORE = False
    INPUT_FILES = [get_data_file("example.txt")]
    IS_DISTRIBUTED = True
    RESOLVED_IS_DISTRIBUTED = True


class TestQuickTxt(PbTestApp):
    """Runs dev_qhello_world """
    DRIVER_EMIT = "python -m pbcommand.cli.examples.dev_quick_hello_world  emit-tool-contract pbcommand.tasks.dev_txt_hello "
    DRIVER_RESOLVE = "python -m pbcommand.cli.examples.dev_quick_hello_world  run-rtc "

    REQUIRES_PBCORE = False
    INPUT_FILES = [get_data_file("example.txt")]
    IS_DISTRIBUTED = True
    RESOLVED_IS_DISTRIBUTED = False # XXX is_distributed=False in task TC!


class TestQuickCustomTxtCustomOuts(PbTestApp):
    """Runs dev_qhello_world """
    DRIVER_EMIT = "python -m pbcommand.cli.examples.dev_quick_hello_world  emit-tool-contract pbcommand.tasks.dev_txt_custom_outs "
    DRIVER_RESOLVE = "python -m pbcommand.cli.examples.dev_quick_hello_world  run-rtc "

    REQUIRES_PBCORE = False
    INPUT_FILES = [get_data_file("example.txt")]


class TestOptionTypes(PbTestApp):
    DRIVER_BASE = "python -m pbcommand.cli.examples.dev_mixed_app"
    REQUIRES_PBCORE = False
    INPUT_FILES = [get_data_file("example.txt")]
    TASK_OPTIONS = {
        "pbcommand.task_options.alpha": 50,
        "pbcommand.task_options.beta": 9.876,
        "pbcommand.task_options.gamma": False,
        "pbcommand.task_options.ploidy": "diploid"
    }
    RESOLVED_TASK_OPTIONS = {
        "pbcommand.task_options.alpha": 50,
        "pbcommand.task_options.beta": 9.876,
        "pbcommand.task_options.gamma": False,
        "pbcommand.task_options.ploidy": "diploid",
        "pbcommand.task_options.delta": 1,
        "pbcommand.task_options.epsilon": 0.1
    }


class TestBadChoiceValue(TestOptionTypes):
    TASK_OPTIONS = {
        "pbcommand.task_options.alpha": 50,
        "pbcommand.task_options.beta": 9.876,
        "pbcommand.task_options.gamma": False,
        "pbcommand.task_options.ploidy": "other"
    }

    def test_run_e2e(self):
        self.assertRaises(ToolContractError, super(TestBadChoiceValue, self).test_run_e2e)


class TestQuickOptionTypes(PbTestApp):
    DRIVER_EMIT = "python -m pbcommand.cli.examples.dev_quick_hello_world  emit-tool-contract pbcommand.tasks.dev_test_options"
    DRIVER_RESOLVE = "python -m pbcommand.cli.examples.dev_quick_hello_world run-rtc "
    INPUT_FILES = [get_data_file("example.txt")]
    TASK_OPTIONS = {
        "pbcommand.task_options.alpha": 50,
        "pbcommand.task_options.beta": 9.876,
        "pbcommand.task_options.gamma": False,
        "pbcommand.task_options.ploidy": "diploid"
    }
    RESOLVED_TASK_OPTIONS = {
        "pbcommand.task_options.alpha": 50,
        "pbcommand.task_options.beta": 9.876,
        "pbcommand.task_options.gamma": False,
        "pbcommand.task_options.ploidy": "diploid",
        "pbcommand.task_options.delta": 1,
        "pbcommand.task_options.epsilon": 0.01
    }
