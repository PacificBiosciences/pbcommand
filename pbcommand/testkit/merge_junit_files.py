"""
Utility to combine individual JUnit XML files.
"""

from __future__ import absolute_import, division, print_function

import argparse
import logging
import os
import sys

from pbcommand.cli import (get_default_argparser_with_base_opts,
                           pacbio_args_runner)
from pbcommand.utils import setup_log

from pbcommand.testkit import xunit

log = logging.getLogger(__name__)


def run(args):
    xunit.merge_junit_files(args.output_file, args.junit_file)
    return 0


def _get_parser():
    p = get_default_argparser_with_base_opts(
        version="0.1",
        description=__doc__)
    p.add_argument("junit_file", nargs="+", type=argparse.FileType('r'))
    p.add_argument("-o", "--output-file", dest="output_file", action="store",
                   default="junit_results_merged.xml")
    return p


def main(argv=sys.argv):
    return pacbio_args_runner(
        argv=argv[1:],
        parser=_get_parser(),
        args_runner_func=run,
        alog=log,
        setup_log_func=setup_log)


if __name__ == "__main__":
    sys.exit(main())
