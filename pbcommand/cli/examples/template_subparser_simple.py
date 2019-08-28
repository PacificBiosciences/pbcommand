from __future__ import print_function
import sys
import logging

from pbcommand.utils import setup_log
from pbcommand.cli.core import get_default_argparser
from pbcommand.cli.utils import subparser_builder, args_executer, main_runner
from pbcommand.common_options import add_base_options

log = logging.getLogger(__name__)

__version__ = "0.1.0"


def _alpha_main(a, b):
    # Core main func, should be imported from lib code
    print("Running {} {}".format(a, b))
    return 0


def _alpha_args_runner(args):
    # translate IO layer to decouple the subprocessor main and CLI interface
    log.info("Running alpha with args {}".format(args))
    return _alpha_main(args.a, args.b)


def _alpha_add_options(p):
    p.add_argument("a", help="A", type=int, default=1)
    p.add_argument("b", help="b", type=int, default=2)
    return p


def _alpha_add_options_with_base_options(p):
    return _alpha_add_options(add_base_options(p))


def _beta_main(name):
    print("Hello {}".format(name))
    return 0


def _beta_args_runner(args):
    return _beta_main(args.name)


def _beta_add_options(p):
    p.add_argument("name", help="User Name", type=str, default=1)
    return p


def get_parser():
    desc = "Example Subparser"

    #p = get_default_argparser_with_base_opts(__version__, desc)

    # This will add logging options and impacts order of args
    # Example `template_subparser_simple.py alpha 1 2 --debug`
    # The common use case will be to add base options to the subparser instance
    # using the add_base_options func

    p = get_default_argparser(__version__, desc)

    sp = p.add_subparsers(help='commands')

    def builder(subparser_id, description, options_func, exe_func):
        subparser_builder(sp, subparser_id, description, options_func, exe_func)

    alpha_desc = "Subcommand Parser 1 (Alpha)"
    builder("alpha", alpha_desc, _alpha_add_options_with_base_options, _alpha_args_runner)

    beta_desc = "Subcommand Parser 2 (Beta)"
    # see above comments concerning adding "base" options.
    builder("beta", beta_desc, _beta_add_options, _beta_args_runner)

    return p


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()

    return main_runner(argv_[1:], parser, args_executer, setup_log, log)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
