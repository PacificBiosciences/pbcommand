"""Demonstration Example app

Primaryly used for end-to-end testing.
emit tool contract -> Resolve -> resolved tool contract -> run.
"""

import logging
import sys

from pbcommand.utils import setup_log
from pbcommand.cli import pacbio_args_or_contract_runner_emit
from pbcommand.models import TaskTypes, FileTypes, get_default_contract_parser

TOOL_ID = "pbcommand.tasks.dev_txt_app"
VERSION = "0.1.0"

log = logging.getLogger(__name__)


def get_parser():
    driver_exe = "python -m pbcommand.cli.example.dev_app --resolved-tool-contract "
    desc = "Dev app for Testing that supports emitting tool contracts"
    task_type = TaskTypes.LOCAL
    p = get_default_contract_parser(TOOL_ID, VERSION, desc, driver_exe, task_type, 1, ())
    p.add_input_file_type(FileTypes.TXT, "txt_in", "Txt file", "Generic Text File")
    p.add_output_file_type(FileTypes.TXT, "txt_out", "Txt outfile", "Generic Output Txt file", "output.txt")
    return p


def run_main(input_txt, output_txt):
    n = 0
    with open(input_txt, 'r') as r:
        with open(output_txt, 'w') as w:
            w.write("# Output Txt file")
            for line in r:
                w.write(line + "\n")
                n += 1

    log.info("Completed writing {n} lines".format(n=n))
    return 0


def args_runner(args):
    return run_main(args.txt_in, args.txt_out)


def rtc_runner(rtc):
    return run_main(rtc.task.input_files[0], rtc.task.output_files[0])


def main(argv=sys.argv):
    return pacbio_args_or_contract_runner_emit(argv[1:],
                                               get_parser(),
                                               args_runner,
                                               rtc_runner,
                                               log,
                                               setup_log)


if __name__ == '__main__':
    sys.exit(main())