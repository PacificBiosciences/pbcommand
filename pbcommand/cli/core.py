""" New Commandline interface that supports ResolvedToolContracts and emitting ToolContracts

There's three use cases.

- running from an argparse instance
- running from a Resolved Tool Contract (RTC)
- emitting a ToolContract (TC)

Going to do this in a new steps.

- de-serializing of RTC (I believe this should be done via avro, not a new random JSON file. With avro, the java, c++, classes can be generated. Python can load the RTC via a structure dict that has a well defined schema)
- get loading and running of RTC from commandline to call main func in a report.
- generate/emit TC from a a common commandline parser interface that builds the TC and the standard argparse instance


"""
import argparse
import json
import logging
import time
import traceback
import sys

import pbcommand

from pbcommand.models import PbParser
from pbcommand.common_options import (RESOLVED_TOOL_CONTRACT_OPTION,
                                      EMIT_TOOL_CONTRACT_OPTION,
                                      add_resolved_tool_contract_option,
                                      add_base_options)

from pbcommand.pb_io.tool_contract_io import load_resolved_tool_contract_from


def _add_version(p, version):
    p.version = version
    p.add_argument('--version',
                   action="version",
                   help="show program's version number and exit")
    return p


def get_default_argparser(version, description):
    """
    Everyone should use this to create an instance on a argparser python parser.


    *This should be replaced updated to have the required base options*

    :param version: Version of your tool
    :param description: Description of your tool
    :return:
    :rtype: ArgumentParser
    """
    p = argparse.ArgumentParser(description=description,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    # Explicitly adding here to have only --version (not -v)
    return _add_version(p, version)


def get_default_argparser_with_base_opts(version, description, default_level="INFO"):
    """Return a parser with the default log related options

    If you don't want the default log behavior to go to stdout, then set
    the default log level to be "ERROR". This will essentially suppress all
    output to stdout.

    Default behavior will only emit to stderr. This is essentially a '--quiet'
    default mode.

    my-tool --my-opt=1234 file_in.txt

    To override the default behavior and add a chatty-er stdout

    my-tool --my-opt=1234 --log-level=INFO file_in.txt

    Or write the console output to write the log file to an explict file and
    leave the stdout untouched.

    my-tool --my-opt=1234 --log-level=DEBUG --log-file=file.log file_in.txt

    """
    return add_base_options(get_default_argparser(version, description), default_level=default_level)


def __get_parsed_args_log_level(pargs, default_level=logging.INFO):
    level = default_level
    if hasattr(pargs, 'verbosity') and pargs.verbosity > 0:
        if pargs.verbosity >= 2:
            level = logging.DEBUG
        else:
            level = logging.INFO
    elif hasattr(pargs, 'debug') and pargs.debug:
        level = logging.DEBUG
    elif hasattr(pargs, 'quiet') and pargs.quiet:
        level = logging.ERROR
    elif hasattr(pargs, 'log_level'):
        level = logging.getLevelName(pargs.log_level)
    return level


def _pacbio_main_runner(alog, setup_log_func, exe_main_func, *args, **kwargs):
    """
    Runs a general func and logs results. The return type is expected to be an (int) return code.

    :param alog: a log instance

    :param func: a cli exe func, must return an int exit code. func(args) => Int, where args is parsed from p.parse_args()

    :param args: parsed args from parser

    :param setup_log_func: F(alog, level=value, file_name=value, formatter=value) or None

    :return: Exit code of callable func
    :rtype: int
    """

    started_at = time.time()

    pargs = args[0]
    # default logging level
    level = logging.INFO

    if 'level' in kwargs:
        level = kwargs.pop('level')
    else:
        level = __get_parsed_args_log_level(pargs)

    # None will default to stdout
    log_file = getattr(pargs, 'log_file', None)

    # Currently, only support to stdout. More customization would require
    # more required commandline options in base parser (e.g., --log-file, --log-formatter)
    log_options = dict(level=level, file_name=log_file)

    # The Setup log func must adhere to the pbcommand.utils.setup_log func
    # signature
    # FIXME. This should use the more concrete F(file_name_or_name, level, formatter)
    # signature of setup_logger
    if setup_log_func is not None and alog is not None:
        setup_log_func(alog, **log_options)
        alog.info("Using pbcommand v{v}".format(v=pbcommand.get_version()))
        alog.info("completed setting up logger with {f}".format(f=setup_log_func))
        alog.info("log opts {d}".format(d=log_options))

    try:
        # the code in func should catch any exceptions. The try/catch
        # here is a fail safe to make sure the program doesn't fail
        # and the makes sure the exit code is logged.
        return_code = exe_main_func(*args, **kwargs)
        run_time = time.time() - started_at
    except Exception as e:
        run_time = time.time() - started_at
        if alog is not None:
            alog.error(e, exc_info=True)
        traceback.print_exc(sys.stderr)

        # We should have a standard map of exit codes to Int
        if isinstance(e, IOError):
            return_code = 1
        else:
            return_code = 2

    _d = dict(r=return_code, s=run_time)
    if alog is not None:
        alog.info("exiting with return code {r} in {s:.2f} sec.".format(**_d))
    return return_code


def pacbio_args_runner(argv, parser, args_runner_func, alog, setup_log_func):
    # For tools that haven't yet implemented the ToolContract API
    args = parser.parse_args(argv)
    return _pacbio_main_runner(alog, setup_log_func, args_runner_func, args)


def pacbio_args_or_contract_runner(argv,
                                   parser,
                                   args_runner_func,
                                   contract_tool_runner_func,
                                   alog, setup_log_func):
    """
    For tools that understand resolved_tool_contracts, but can't emit
    tool contracts (they may have been written by hand)

    :param parser: argparse Parser
    :type parser: ArgumentParser

    :param args_runner_func: func(args) => int signature

    :param contract_tool_runner_func: func(tool_contract_instance) should be
    the signature

    :param alog: a python log instance
    :param setup_log_func: func(log_instance) => void signature
    :return: int return code
    :rtype: int
    """
    def _log_not_none(msg):
        if alog is not None:
            alog.info(msg)

    # circumvent the argparse parsing by inspecting the raw argv, then create
    # a temporary parser with limited arguments to process the special case of
    # --resolved-cool-contract (while still respecting verbosity flags).
    if any(a.startswith(RESOLVED_TOOL_CONTRACT_OPTION) for a in argv):
        p_tmp = get_default_argparser(version=parser.version,
            description=parser.description)
        add_resolved_tool_contract_option(add_base_options(p_tmp,
            default_level="NOTSET"))
        args_tmp = p_tmp.parse_args(argv)
        resolved_tool_contract = load_resolved_tool_contract_from(
            args_tmp.resolved_tool_contract)
        _log_not_none("Successfully loaded resolved tool contract from {a}".format(a=argv))
        # XXX if one of the logging flags was specified, that takes precedence,
        # otherwise use the log level in the resolved tool contract.  note that
        # this takes advantage of the fact that argparse allows us to use
        # NOTSET as the default level even though it's not one of the choices.
        log_level = __get_parsed_args_log_level(args_tmp,
            default_level=logging.NOTSET)
        if log_level == logging.NOTSET:
            log_level = resolved_tool_contract.task.log_level
        r = _pacbio_main_runner(alog, setup_log_func, contract_tool_runner_func, resolved_tool_contract, level=log_level)
        _log_not_none("Completed running resolved contract. {c}".format(c=resolved_tool_contract))
        return r
    else:
        # tool was called with the standard commandline invocation
        return pacbio_args_runner(argv, parser, args_runner_func, alog,
                                  setup_log_func)


def pbparser_runner(argv,
                    parser,
                    args_runner_func,
                    contract_runner_func,
                    alog,
                    setup_log_func):
    """Run a Contract or emit a contract to stdout."""
    if not isinstance(parser, PbParser):
        raise TypeError("Only supports PbParser.")

    arg_parser = parser.arg_parser.parser
    # extract the contract
    tool_contract = parser.to_contract()

    if EMIT_TOOL_CONTRACT_OPTION in argv:
        # print tool_contract
        x = json.dumps(tool_contract.to_dict(), indent=4)
        print x
    else:
        return pacbio_args_or_contract_runner(argv, arg_parser, args_runner_func, contract_runner_func, alog, setup_log_func)
