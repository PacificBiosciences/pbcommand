"""Simple CLI dev app for testing Emitting Tool Contracts and Running from Resolved Tool Contracts"""

import logging
import sys

from pbcommand.utils import setup_log
from pbcommand.cli import pbparser_runner
from pbcommand.models import TaskTypes, FileTypes, get_pbparser, ResourceTypes


# This has the same functionality as the dev_simple_app
from .dev_simple_app import run_main

log = logging.getLogger(__name__)

__version__ = '0.2.1'

# Used for the tool contract id. Must have the form {namespace}.tasks.{name}
# to prevent namespace collisions. For python tools, the namespace should be
# the python package name.
TOOL_ID = "pbcommand.tasks.dev_app"


def add_args_and_options(p):
    """
    Add input, output files and options to parser.

    :type p: PbParser
    :return: PbParser
    """
    # FileType, label, name, description
    p.add_input_file_type(FileTypes.FASTA, "fasta_in", "Fasta File", "PacBio Spec'ed fasta file")
    # File Type, label, name, description, default file name
    p.add_output_file_type(FileTypes.FASTA, "fasta_out", "Filtered Fasta file", "Filtered Fasta file", "filter.fasta")
    # Option id, label, default value, name, description
    # for the argparse, the read-length will be translated to --read-length and (accessible via args.read_length)
    p.add_int("pbcommand.task_options.dev_read_length", "read-length", 25, "Length filter", "Min Sequence Length filter")
    return p


def get_contract_parser():
    """
    Central point of programmatically defining a Parser.
    :rtype: PbParser
    :return: PbParser
    """
    # Commandline exe to call "{exe}" /path/to/resolved-tool-contract.json

    driver_exe = "python -m pbcommand.cli.example.dev_app --resolved-tool-contract "
    desc = "Dev app for Testing that supports emitting tool contracts"
    subcomponents = [("my_subcomponent", "1.2.3")]

    resource_types = (ResourceTypes.TMP_FILE,
                      ResourceTypes.TMP_FILE,
                      ResourceTypes.TMP_DIR)

    p = get_pbparser(TOOL_ID,
                     __version__,
                     "Example Dev App",
                     desc,
                     driver_exe,
                     is_distributed=False,
                     resource_types=resource_types,
                     subcomponents=subcomponents)

    add_args_and_options(p)
    return p


def args_runner(args):
    """Entry point from argparse"""
    log.debug("raw args {a}".format(a=args))
    return run_main(args.fasta_in, args.fasta_out, args.read_length)


def resolved_tool_contract_runner(resolved_tool_contract):
    """Run from the resolved contract

    :param resolved_tool_contract:
    :type resolved_tool_contract: ResolvedToolContract
    """

    in_file = resolved_tool_contract.task.input_files[0]
    out_file = resolved_tool_contract.task.output_files[0]
    min_read_length = resolved_tool_contract.task.options["pbcommand.task_options.dev_read_length"]
    r = run_main(in_file, out_file, min_read_length)
    return r


def main(argv=sys.argv):
    log.info("Starting {f} version {v} pbcommand example dev app".format(f=__file__, v=__version__))
    # PbParser instance, this has both the argparse instance and the tool contract
    # instance.
    mp = get_contract_parser()
    # To Access the argparse instance
    # mp.arg_parser.parser
    # The Tool Contract parser
    # mp.tool_contract_parser.parser
    return pbparser_runner(argv[1:],
                           mp,
                           args_runner,
                           resolved_tool_contract_runner,
                           log,
                           setup_log)


if __name__ == '__main__':
    sys.exit(main())
