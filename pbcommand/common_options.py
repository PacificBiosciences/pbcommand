"""Common options and utils that can me used in commandline utils"""
import argparse
import logging
import sys

from pbcommand.utils import compose

RESOLVED_TOOL_CONTRACT_OPTION = "--resolved-tool-contract"
EMIT_TOOL_CONTRACT_OPTION = "--emit-tool-contract"


def add_debug_option(p):
    p.add_argument('--debug', action="store_true", default=False, help="Debug to stdout")
    return p


def add_log_level_option(p):
    p.add_argument('--log-level', choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'),
                   default='INFO', help="Set log level")
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
    funcs = [add_debug_option,
             add_log_level_option]
    fs = compose(*funcs)
    return fs(p)


def add_base_options_with_emit_tool_contract(p):
    funcs = [add_base_options,
             add_resolved_tool_contract_option,
             add_emit_tool_contract_option]
    fs = compose(*funcs)
    return fs(p)


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
