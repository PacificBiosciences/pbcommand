"""
New Commandline interface that supports ResolvedToolContracts and emitting ToolContracts

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
import errno
import json
import logging
import os
import shutil
import sys
import time
import traceback

import pbcommand
from pbcommand.models import ResourceTypes, PacBioAlarm
from pbcommand.models.report import Report, Attribute
from pbcommand.common_options import add_base_options, add_nproc_option
from pbcommand.utils import get_parsed_args_log_level, get_peak_memory_usage


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


def get_default_argparser_with_base_opts(
        version, description, default_level="INFO", nproc=None):
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
    p = add_base_options(
        get_default_argparser(
            version,
            description),
        default_level=default_level)
    if nproc is not None:
        p = add_nproc_option(p)
    return p


def write_task_report(run_time, nproc, exit_code, maxrss):
    attributes = [
        Attribute("host", value=os.uname()[1]),
        Attribute("system", value=os.uname()[0]),
        Attribute("nproc", value=nproc),
        Attribute("run_time", value=run_time),
        Attribute("exit_code", value=exit_code),
        Attribute("maxrss", value=maxrss)
    ]
    report = Report("workflow_task",
                    title="Workflow Task Report",
                    attributes=attributes,
                    tags=("internal",))
    report.write_json("task-report.json")


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
        level = get_parsed_args_log_level(pargs)

    # None will default to stdout
    log_file = getattr(pargs, 'log_file', None)

    # Currently, only support to stdout. More customization would require
    # more required commandline options in base parser (e.g., --log-file,
    # --log-formatter)
    log_options = dict(level=level, file_name=log_file)

    base_dir = os.getcwd()

    dump_alarm_on_error = False
    if "dump_alarm_on_error" in kwargs:
        dump_alarm_on_error = kwargs.pop("dump_alarm_on_error")
    is_cromwell_environment = bool(
        os.environ.get(
            "SMRT_PIPELINE_BUNDLE_DIR",
            None)) and "cromwell-executions" in base_dir
    dump_alarm_on_error = dump_alarm_on_error and is_cromwell_environment

    # The Setup log func must adhere to the pbcommand.utils.setup_log func
    # signature
    # FIXME. This should use the more concrete F(file_name_or_name, level, formatter)
    # signature of setup_logger
    if setup_log_func is not None and alog is not None:
        setup_log_func(alog, **log_options)
        alog.info("Using pbcommand v{v}".format(v=pbcommand.get_version()))
        alog.info(
            "completed setting up logger with {f}".format(
                f=setup_log_func))
        alog.info("log opts {d}".format(d=log_options))

    if dump_alarm_on_error:
        alog.info(
            "This command appears to be running as part of a Cromwell workflow")
        alog.info("Additional output files may be generated")

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
        else:
            traceback.print_exc(sys.stderr)
        if dump_alarm_on_error:
            PacBioAlarm.dump_error(
                file_name=os.path.join(base_dir, "alarms.json"),
                exception=e,
                info="".join(traceback.format_exc()),
                message=str(e),
                name=e.__class__.__name__,
                severity=logging.ERROR)

        # We should have a standard map of exit codes to Int
        if isinstance(e, IOError):
            return_code = 1
        else:
            return_code = 2

    maxrss = get_peak_memory_usage()
    if is_cromwell_environment:
        alog.info("Writing task report to task-report.json")
        nproc = getattr(pargs, "nproc", 1)
        write_task_report(run_time, nproc, return_code, maxrss)
    _d = dict(r=return_code, s=run_time)
    if alog is not None:
        alog.info(f"Max RSS (kB): {maxrss}")
        alog.info("exiting with return code {r} in {s:.2f} sec.".format(**_d))
    return return_code


def pacbio_args_runner(argv, parser, args_runner_func, alog, setup_log_func,
                       dump_alarm_on_error=True):
    # For tools that haven't yet implemented the ToolContract API
    args = parser.parse_args(argv)
    return _pacbio_main_runner(alog, setup_log_func, args_runner_func, args,
                               dump_alarm_on_error=dump_alarm_on_error)
