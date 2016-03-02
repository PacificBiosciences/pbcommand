
"""
Additional utilities for running command-line apps - most of these do not apply
to tool-contract-driven programs.  (Ported from pbsmrtpipe)
"""

import traceback
import argparse
import platform
import logging
import time
import os

from pbcommand.validators import validate_file, validate_fofn
from pbcommand.utils import setup_log

log = logging.getLogger(__name__)


def subparser_builder(subparser, subparser_id, description, options_func, exe_func):
    """
    Util to add subparser options

    :param subparser:
    :param subparser_id:
    :param description:
    :param options_func: Function that will add args and options to Parser instance F(subparser) -> None
    :param exe_func: Function to run F(args) -> Int
    :return:
    """
    p = subparser.add_parser(subparser_id, help=description)
    options_func(p)
    p.set_defaults(func=exe_func)
    return p


def add_debug_option(p):
    p.add_argument('--debug', action='store_true',
                   help="Send logging info to stdout.")
    return p


def _validate_output_dir_or_get_default(value):
    if value is None:
        return os.getcwd()
    else:
        if os.path.exists(value):
            return os.path.abspath(value)
        else:
            os.mkdir(value)
            return os.path.abspath(value)


def add_output_dir_option(p):
    p.add_argument('-o', '--output-dir', type=_validate_output_dir_or_get_default, default=os.getcwd(), help="Output directory.")
    return p


def _add_input_file(args_label, type_, help_):
    def _wrapper(p):
        p.add_argument(args_label, type=type_, help=help_)
        return p
    return _wrapper


add_fasta_output = _add_input_file("fasta_out", str, "Path to output Fasta File")
add_fasta_input = _add_input_file("fasta_in", validate_file, "Path to Input FASTA File")

add_fastq_output = _add_input_file("fastq_out", str, "Path to output Fastq File")
add_fastq_input = _add_input_file("fastq_in", validate_file, "Path to Input FASTQ File")

add_fofn_input = _add_input_file("fofn_in", validate_fofn, "Path to Input FOFN (File of file names) File")
add_fofn_output = _add_input_file("fofn_out", str, "Path to output FOFN.")

add_report_output = _add_input_file("json_report", str, "Path to PacBio JSON Report")

add_subread_input = _add_input_file("subread_ds", validate_file, "Path to PacBio Subread DataSet XML")

add_ds_reference_input = _add_input_file("reference_ds", validate_file, "Path to PacBio Subread DataSet XML")


def args_executer(args):
    """


    :rtype int
    """
    try:
        return_code = args.func(args)
    except Exception as e:
        log.error(e, exc_info=True)
        traceback.print_exc(sys.stderr)
        if isinstance(e, IOError):
            return_code = 1
        else:
            return_code = 2

    return return_code


def main_runner(argv, parser, exe_runner_func, setup_log_func, alog):
    """
    Fundamental interface to commandline applications
    """
    started_at = time.time()
    args = parser.parse_args(argv)
    # log.debug(args)

    # setup log
    _have_log_setup = False
    if hasattr(args, 'quiet') and args.quiet:
        setup_log_func(alog, level=logging.ERROR)
    elif hasattr(args, 'verbosity') and args.verbosity > 0:
        if args.verbosity >= 2:
            setup_log_func(alog, level=logging.DEBUG)
        else:
            setup_log_func(alog, level=logging.INFO)
    elif hasattr(args, 'debug') and args.debug:
        setup_log_func(alog, level=logging.DEBUG)
    else:
        alog.addHandler(logging.NullHandler())

    log.debug(args)
    alog.info("Starting tool version {v}".format(v=parser.version))
    rcode = exe_runner_func(args)

    run_time = time.time() - started_at
    _d = dict(r=rcode, s=run_time)
    alog.info("exiting with return code {r} in {s:.2f} sec.".format(**_d))
    return rcode


def main_runner_default(argv, parser, alog):
    # FIXME. This still has the old set_defaults(func=func) and
    # has the assumption that --debug has been assigned as an option
    # This is used for all the subparsers
    setup_log_func = setup_log
    return main_runner(argv, parser, args_executer, setup_log_func, alog)
