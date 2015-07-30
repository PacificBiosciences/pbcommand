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

from pbcommand.common_options import (RESOLVED_TOOL_CONTRACT_OPTION,
                                      EMIT_TOOL_CONTRACT_OPTION)
from pbcommand.models import PbParser
from pbcommand.pb_io.tool_contract_io import load_resolved_tool_contract_from


def get_default_argparser(version, description):
    """
    Everyone MUST use this to create an instance on a argparser python parser.

    :param version:
    :param description:
    :return:
    :rtype: ArgumentParser
    """
    p = argparse.ArgumentParser(version=version,
                                description=description,
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    return p


def _pacbio_main_runner(alog, setup_log_func, func, *args, **kwargs):
    """
    Runs a general func and logs results. The return type is expected to be an (int) return code.

    :param alog: a log instance

    :param func: a cli exe func, must return an int exit code. func(args) => Int, where args is parsed from p.parse_args()

    :param args: parsed args from parser

    :return: Exit code of callable func
    :rtype: int
    """

    started_at = time.time()

    pargs = args[0]
    level = logging.INFO
    # Assuming that the log_level might not be an added option.
    if hasattr(pargs, 'log_level'):
        level = logging.getLevelName(pargs.log_level)

    log_options = dict(level=level)
    # The Setup log func must adhere to the pbcommand.utils.setup_log func
    # signature
    setup_log_func(alog, **log_options)

    try:
        # the code in func should catch any exceptions. The try/catch
        # here is a fail safe to make sure the program doesn't fail
        # and the makes sure the exit code is logged.
        return_code = func(*args, **kwargs)
        run_time = time.time() - started_at
    except Exception as e:
        run_time = time.time() - started_at
        alog.error(e, exc_info=True)
        traceback.print_exc(sys.stderr)

        # We should have a standard map of exit codes to Int
        if isinstance(e, IOError):
            return_code = 1
        else:
            return_code = 2

    _d = dict(r=return_code, s=run_time)
    alog.info("exiting with return code {r} in {s:.2f} sec.".format(**_d))
    return return_code


def _get_resolved_tool_contract_from_argv(argv):
    """
    Extract the resolved tool contract path from the raw argv

    There are two cases

    --resolved-tool-contract-path=/path/to/tool_contract.json
    --resolved-tool-contract-path /path/to/tool_contract.json

    :param argv:
    :rtype: str
    :raises: ValueError
    :return: Path to Manifest
    """
    # this is a lackluster implementation. FIXME.

    m_str = RESOLVED_TOOL_CONTRACT_OPTION

    error = ValueError("Unable to extract resolved tool contract from commandline args {a}. Expecting {m}=/path/to/file.json".format(a=argv, m=m_str))
    tool_contract_path = None
    nargv = len(argv)

    # Provided the --resolved-tool-contract /path/to/tool_contract_path.json
    if m_str in argv:
        for i, a in enumerate(argv):
            # print i, nargv, a
            if a.startswith(m_str):
                if (i + 1) <= nargv:
                    tool_contract_path = argv[i + 1]
                    break
                else:
                    raise error

    # Provided the --resolved-tool-contract=/path/to/tool_contract_path.json
    m_str_eq = m_str + "="
    for i in argv:
        if i.startswith(m_str_eq):
            tool_contract_path = i.split(m_str_eq)[-1]
            break

    if tool_contract_path is None:
        raise error

    return tool_contract_path


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

    # circumvent the argparse parsing by inspecting the raw argv, then manually
    # parse out the resolved_tool_contract path. Not awesome, but the only way to skip the
    # parser.parse_args(args) machinery
    if any(a.startswith(RESOLVED_TOOL_CONTRACT_OPTION) for a in argv):
        print "Attempting to Load resolved tool contract from {a}".format(a=argv)
        # FIXME need to catch the exception if raised here before the _pacbio_main_runner is called
        resolved_tool_contract_path = _get_resolved_tool_contract_from_argv(argv)
        resolved_tool_contract = load_resolved_tool_contract_from(resolved_tool_contract_path)
        r = _pacbio_main_runner(alog, setup_log_func, contract_tool_runner_func, resolved_tool_contract)
        # alog.info("Completed running resolved contract. {c}".format(c=resolved_tool_contract))
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
