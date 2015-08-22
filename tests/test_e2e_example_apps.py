import logging

from base_utils import get_data_file
import pbcommand.testkit

log = logging.getLogger(__name__)


class TestRunDevApp(pbcommand.testkit.PbTestApp):
    DRIVER_BASE = "python -m pbcommand.cli.examples.dev_app "
    REQUIRES_PBCORE = True
    INPUT_FILES = [get_data_file("example.fasta")]
    TASK_OPTIONS = {"pbcommand.task_options.dev_read_length": 27}


class TestTxtDevApp(pbcommand.testkit.PbTestApp):
    DRIVER_BASE = "python -m pbcommand.cli.examples.dev_txt_app "
    # XXX using default args, so the emit/resolve drivers are automatic
    REQUIRES_PBCORE = False
    INPUT_FILES = [get_data_file("example.txt")]
    TASK_OPTIONS = {"pbcommand.task_options.dev_max_nlines": 27}
    RESOLVED_TASK_OPTIONS = {"pbcommand.task_options.dev_max_nlines": 27}


class TestQuickDevHelloWorld(pbcommand.testkit.PbTestApp):
    """Runs dev_qhello_world """
    DRIVER_EMIT = "python -m pbcommand.cli.examples.dev_quick_hello_world  emit-tool-contract pbcommand.tasks.dev_qhello_world "
    DRIVER_RESOLVE = "python -m pbcommand.cli.examples.dev_quick_hello_world  run-rtc "

    REQUIRES_PBCORE = False
    INPUT_FILES = [get_data_file("example.txt")]


class TestQuickTxt(pbcommand.testkit.PbTestApp):
    """Runs dev_qhello_world """
    DRIVER_EMIT = "python -m pbcommand.cli.examples.dev_quick_hello_world  emit-tool-contract pbcommand.tasks.dev_txt_hello "
    DRIVER_RESOLVE = "python -m pbcommand.cli.examples.dev_quick_hello_world  run-rtc "

    REQUIRES_PBCORE = False
    INPUT_FILES = [get_data_file("example.txt")]


class TestQuickCustomTxtCustomOuts(pbcommand.testkit.PbTestApp):
    """Runs dev_qhello_world """
    DRIVER_EMIT = "python -m pbcommand.cli.examples.dev_quick_hello_world  emit-tool-contract pbcommand.tasks.dev_txt_custom_outs "
    DRIVER_RESOLVE = "python -m pbcommand.cli.examples.dev_quick_hello_world  run-rtc "

    REQUIRES_PBCORE = False
    INPUT_FILES = [get_data_file("example.txt")]
