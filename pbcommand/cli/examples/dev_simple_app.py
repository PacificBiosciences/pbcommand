"""Simple CLI dev app for testing

This app is a 'simple' app in that is can not emit tool-contracts, but it
can run tool contracts via --
"""

import logging
import sys
import warnings

from pbcommand.utils import setup_log
from pbcommand.validators import validate_file
from pbcommand.models import ResolvedToolContract
from pbcommand.common_options import add_resolved_tool_contract_option
from pbcommand.cli import pacbio_args_or_contract_runner, get_default_argparser

log = logging.getLogger(__name__)

__version__ = '0.1.1'

TOOL_ID = "pbcommand.tasks.dev_app_simple"

try:
    from pbcore.io import FastaWriter, FastaReader
except ImportError:
    warnings.warn("Example apps require pbcore. Install from https://github.com/PacificBiosciences/pbcore")


def get_parser():
    p = get_default_argparser(__version__, __doc__)
    p.add_argument("fasta_in", type=validate_file, help="Input Fasta")
    p.add_argument("fasta_out", type=str, help="Output Fasta")
    p.add_argument('--read-length', type=int, default=25, help="Min Sequence length to filter")
    add_resolved_tool_contract_option(p)
    # this parser cannot emit a tool contract, but can run from a resolved
    # contract via --resolved-tool-contract /path/to/resolved-tool-contract.json
    return p


def run_main(input_file, output_file, min_sequence_length):
    """
    Main function entry point to your application (this should be imported
    from your library code)

    :rtype int:
    """
    _d = dict(i=input_file, a=min_sequence_length, o=output_file)
    msg = "Running dev_app task. with input:{i} output:{o} and min-length={a}".format(**_d)
    log.info(msg)
    with FastaWriter(output_file) as w:
        with FastaReader(input_file) as r:
            for record in r:
                if len(record.sequence) > min_sequence_length:
                    w.writeRecord(record)
    log.debug("completed running main.")
    return 0


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
    alpha = 9
    r = run_main(in_file, out_file, alpha)
    log.info("Completed running resolved contract. {c}".format(c=resolved_tool_contract))
    return r


def main(argv=sys.argv):
    # New interface that supports running resolved tool contracts
    log.info("Starting {f} version {v} pbcommand example dev app".format(f=__file__, v=__version__))

    p = get_parser()
    return pacbio_args_or_contract_runner(argv[1:], p,
                                          args_runner,
                                          resolved_tool_contract_runner,
                                          log,
                                          setup_log)


if __name__ == '__main__':
    sys.exit(main())
