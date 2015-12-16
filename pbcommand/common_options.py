"""Common options and utils that can me used in commandline utils"""
import argparse
import sys


RESOLVED_TOOL_CONTRACT_OPTION = "--resolved-tool-contract"
EMIT_TOOL_CONTRACT_OPTION = "--emit-tool-contract"


def add_debug_option(p):
    # FIXME. This should be re-purposed to launch a debugger with --ipdb or --pdb
    # logging should be handled via --log-* options
    p.add_argument('--debug', action="store_true", default=False, help="Debug to stdout")
    return p


def add_log_level_option(p, default_level='INFO'):
    """Add logging level with a default value"""
    p.add_argument('--log-level', choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
                   default=default_level, help="Set log level")
    return p


def add_log_file_option(p):
    p.add_argument('--log-file', default=None, type=str,
                   help="Write the log to file. Default(None) will write to stdout.")
    return p


def add_resolved_tool_contract_option(p):
    p.add_argument(RESOLVED_TOOL_CONTRACT_OPTION, type=str,
                   help="Run Tool directly from a PacBio Resolved tool contract")
    return p


def add_emit_tool_contract_option(p):
    p.add_argument(EMIT_TOOL_CONTRACT_OPTION, action="store_true",
                   default=False,
                   help="Emit Tool Contract to stdout")
    return p


def add_base_options(p):
    # This should automatically/required be added to be added from get_default_argparser
    return add_debug_option(add_log_level_option(add_log_file_option(p)))


def add_base_options_with_emit_tool_contract(p):
    # can't use compose here because of circular imports via parser
    return add_base_options(add_resolved_tool_contract_option(add_emit_tool_contract_option(p)))


def _to_print_message_action(msg):

    class PrintMessageAction(argparse.Action):

        """Print message and exit"""

        def __call__(self, parser, namespace, values, option_string=None):
            sys.stdout.write(msg + "\n")
            sys.exit(0)

    return PrintMessageAction


def add_subcomponent_versions_option(p, subcomponents):
    """Add subcomponents to a subparser to provide more information
     about the tools dependencies.

     Subcomponents must be provided as a list of tuples (component, version)
     """
    max_length = max(len(x) for x, _ in subcomponents)
    pad = 2
    msg = "\n" .join([" : ".join([x.rjust(max_length + pad), y]) for x, y in subcomponents])

    action = _to_print_message_action(msg)
    p.add_argument("--versions",
                   nargs=0,
                   help="Show versions of individual components",
                   action=action)

    return p
