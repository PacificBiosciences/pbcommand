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
    p.add_argument('--log-level', default=logging.DEBUG, help="Set log level")
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


def _to_show_action(subcomponents):

    class ShowComponentVersionAction(argparse.Action):
        def __call__(self, parser, namespace, values,
                     option_string=None):
            for c_name, c_version in subcomponents:
                sys.stdout.write("  %s version: %s\n" %
                                 (c_name, c_version))
            sys.exit(0)

    return ShowComponentVersionAction


def add_subcomponent_versions_option(p, subcomponents):
    """Add subcomponents to a subparser to provide more information
     about the tools dependencies.

     Subcomponents must be provided as a list of tuples (component, version)
     """
    action = _to_show_action(subcomponents)
    p.add_argument("--versions",
                   nargs=0,
                   help="Show versions of individual components",
                   action=action)

    return p
