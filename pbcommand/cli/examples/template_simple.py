"""Simple Example Template for creating a CLI tool"""
import os
import sys
import logging

from pbcommand.validators import validate_file
from pbcommand.utils import setup_log
from pbcommand.cli import get_default_argparser_with_base_opts, pacbio_args_runner

log = logging.getLogger(__name__)

__version__ = "0.1.0"
# __author__ = "Add-your-name"


def get_parser():
    """Define Parser. Use the helper methods in validators to validate input"""
    p = get_default_argparser_with_base_opts(__version__, __doc__)
    p.add_argument('path_to_file', type=validate_file, help="Path to File")
    return p


def run_main(path, value=8):
    """
    Main function that should be called. Typically this is imported from your
    library code.

    This should NOT reference args.*
    """
    log.info("Running path {p} with value {v}".format(p=path, v=value))
    log.info("Found path? {t} {p}".format(p=path, t=os.path.exists(path)))
    return 0


def args_runner(args):
    log.info("Raw args {a}".format(a=args))
    return run_main(args.path_to_file, value=100)


def main(argv):
    return pacbio_args_runner(argv[1:],
                              get_parser(),
                              args_runner,
                              log,
                              setup_log_func=setup_log)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
