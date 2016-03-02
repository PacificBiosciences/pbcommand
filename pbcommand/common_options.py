"""Common options and utils that can me used in commandline utils"""
import argparse
import sys


RESOLVED_TOOL_CONTRACT_OPTION = "--resolved-tool-contract"
EMIT_TOOL_CONTRACT_OPTION = "--emit-tool-contract"


def add_debug_option(p):
    p.add_argument("--pdb", action="store_true", default=False,
                   help="Enable Python debugger")
    return p


def add_log_debug_option(p):
    """This requires the log-level option"""
    p.add_argument('--debug', action="store_true", default=False, help="Alias for setting log level to DEBUG")
    return p


def add_log_quiet_option(p):
    """This requires the log-level option"""
    p.add_argument('--quiet', action="store_true", default=False, help="Alias for setting log level to CRITICAL to suppress output.")
    return p


def add_log_verbose_option(p):
    p.add_argument(
        "-v",
        "--verbose",
        dest="verbosity",
        action="count",
        help="Set the verbosity level.")
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


def add_base_options(p, default_level='INFO'):
    """Add the core logging options to the parser and set the default log level

    If you don't want the default log behavior to go to stdout, then set
    the default log level to be "ERROR". This will essentially suppress all
    output to stdout.

    Default behavior will only emit to stderr. This is essentially a '--quiet'
    default mode.

    my-tool --my-opt=1234 file_in.txt

    To override the default behavior:

    my-tool --my-opt=1234 --log-level=INFO file_in.txt

    Or write the file to an explict log file

    my-tool --my-opt=1234 --log-level=DEBUG --log-file=file.log file_in.txt

    """
    # This should automatically/required be added to be added from get_default_argparser
    add_log_file_option(p)
    p_log = p.add_mutually_exclusive_group()
    add_log_verbose_option(add_log_quiet_option(add_log_debug_option(
        add_log_level_option(p_log, default_level=default_level))))
    return p


def add_common_options(p, default_level='INFO'):
    """
    New model for 3.1 release. This should replace add_base_options
    """
    return add_log_quiet_option(add_log_debug_option(add_log_level_option(add_log_file_option(p), default_level=default_level)))


def add_base_options_with_emit_tool_contract(p, default_level='INFO'):
    # can't use compose here because of circular imports via parser
    return add_base_options(add_resolved_tool_contract_option(add_emit_tool_contract_option(p)), default_level=default_level)


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
