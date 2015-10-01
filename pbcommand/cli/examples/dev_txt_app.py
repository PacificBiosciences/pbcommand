"""Demonstration Example app

Primaryly used for end-to-end testing.
emit tool contract -> Resolve -> resolved tool contract -> run.
"""

import logging
import sys

from pbcommand.utils import setup_log
from pbcommand.cli import pbparser_runner
from pbcommand.models import TaskTypes, FileTypes, get_pbparser, ResourceTypes

TOOL_ID = "pbcommand.tasks.dev_txt_app"
VERSION = "0.1.0"

log = logging.getLogger(__name__)


def get_parser():
    driver_exe = "python -m pbcommand.cli.examples.dev_app --resolved-tool-contract "
    desc = "Dev app for Testing that supports emitting tool contracts"
    # Can specify libs or other dependencies that
    subcomponents = [("pbcommand", VERSION),
                     ("my_component", "0.1.0"),
                     ("my_component_id", "1.2.3")]
    # ResourceTypes.*
    resource_types = (ResourceTypes.TMP_FILE,
                      ResourceTypes.TMP_FILE,
                      ResourceTypes.TMP_DIR)

    # Create an instance of a Pacbio Parser
    p = get_pbparser(TOOL_ID, VERSION, "Txt App", desc, driver_exe,
                     is_distributed=False, resource_types=resource_types,
                     subcomponents=subcomponents)

    # Add Input Files types
    p.add_input_file_type(FileTypes.TXT, "txt_in", "Txt file", "Generic Text File")
    # Add output files types
    p.add_output_file_type(FileTypes.TXT, "txt_out", "Txt outfile", "Generic Output Txt file", "output.txt")
    p.add_int("pbcommand.task_options.dev_max_nlines", "max_nlines", 10, "Max Lines", "Max Number of lines to Copy")
    return p


def run_main(input_txt, output_txt, max_nlines):
    n = 0
    with open(input_txt, 'r') as r:
        with open(output_txt, 'w') as w:
            w.write("# Output Txt file")
            for line in r:
                if n >= max_nlines:
                    break
                w.write(line + "\n")
                n += 1

    log.info("Completed writing {n} lines".format(n=n))
    return 0


def args_runner(args):
    return run_main(args.txt_in, args.txt_out, args.max_nlines)


def rtc_runner(rtc):
    return run_main(rtc.task.input_files[0],
                    rtc.task.output_files[0],
                    rtc.task.options["pbcommand.task_options.dev_max_nlines"])


def main(argv=sys.argv):
    return pbparser_runner(argv[1:],
                           get_parser(),
                           args_runner,
                           rtc_runner,
                           log,
                           setup_log)


if __name__ == '__main__':
    sys.exit(main())
