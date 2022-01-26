"""
Utils for common funcs, such as setting up a log, composing functions.
"""

import argparse
import functools
import logging
import logging.config
import multiprocessing
import os
import pprint
import subprocess
import sys
import time
import traceback
import types
import xml.etree.ElementTree as ET
from contextlib import contextmanager

from pbcommand.models import FileTypes, DataSetMetaData
from pbcommand import to_ascii


log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())  # suppress the annoying no handlers msg


class Constants:
    """Log Level format strings"""
    LOG_FMT_ONLY_MSG = '%(message)s'
    LOG_FMT_ERR = '%(message)s'
    LOG_FMT_LVL = '[%(levelname)s] %(message)s'
    LOG_FMT_MIN = '[%(asctime)-15sZ] %(message)s'
    LOG_FMT_SIMPLE = '[%(levelname)s] %(asctime)-15sZ %(message)s'
    LOG_FMT_STD = '[%(levelname)s] %(asctime)-15sZ [%(name)s] %(message)s'
    LOG_FMT_FULL = '[%(levelname)s] %(asctime)-15sZ [%(name)s %(funcName)s %(lineno)d] %(message)s'


class ExternalCommandNotFoundError(Exception):
    """External command is not found in Path"""
    pass


def _handler_stream_d(stream, level_str, formatter_id):
    d = {'level': level_str,
         'class': "logging.StreamHandler",
         'formatter': formatter_id,
         'stream': stream}
    return d


_handler_stdout_stream_d = functools.partial(
    _handler_stream_d, "ext://sys.stdout")
_handler_stderr_stream_d = functools.partial(
    _handler_stream_d, "ext://sys.stderr")


def _handler_file(level_str, path, formatter_id):
    d = {'class': 'logging.FileHandler',
         'level': level_str,
         'formatter': formatter_id,
         'filename': path}
    return d


def _get_default_logging_config_dict(level, file_name_or_none, formatter):
    """
    Setup a logger to either a file or console. If file name is none, then
    a logger will be setup to stdout.

    :note: adds console

    Returns a dict configuration of the logger.
    """

    level_str = logging.getLevelName(level)

    formatter_id = 'custom_logger_fmt'
    console_handler_id = "console_handler"

    error_fmt_id = "error_fmt_id"
    error_handler_id = "error_handler"
    error_handler_d = _handler_stderr_stream_d(logging.ERROR, error_fmt_id)

    if file_name_or_none is None:
        handler_d = _handler_stdout_stream_d(level_str, formatter_id)
    else:
        handler_d = _handler_file(level_str, file_name_or_none, formatter_id)

    formatters_d = {fid: {'format': fx} for fid, fx in [
        (formatter_id, formatter), (error_fmt_id, Constants.LOG_FMT_ERR)]}

    handlers_d = {console_handler_id: handler_d,
                  error_handler_id: error_handler_d}

    loggers_d = {"custom": {'handlers': [console_handler_id],
                            'stderr': {'handlers': [error_handler_id]}}}

    d = {
        'version': 1,
        'disable_existing_loggers': False,  # this fixes the problem
        'formatters': formatters_d,
        'handlers': handlers_d,
        'loggers': loggers_d,
        'root': {'handlers': [error_handler_id, console_handler_id], 'level': logging.NOTSET}
    }

    # print pprint.pformat(d)
    return d


def _get_console_and_file_logging_config_dict(
        console_level, console_formatter, path, path_level, path_formatter):
    """
    Get logging configuration that is both for console and a file.

    :note: A stderr logger handler is also added.

    """

    def _to_handler_d(handlers_, level):
        return {"handlers": handlers_, "level": level, "propagate": True}

    console_handler_id = "console_handler"
    console_fmt_id = "console_fmt"
    console_handler_d = _handler_stdout_stream_d(console_level, console_fmt_id)

    stderr_handler_id = "stderr_handler"
    error_fmt_id = "error_fmt"
    stderr_handler_d = _handler_stderr_stream_d(logging.ERROR, console_fmt_id)

    file_handler_id = "file_handler"
    file_fmt_id = "file_fmt"
    file_handler_d = _handler_file(path_level, path, file_fmt_id)

    formatters = {console_fmt_id: {"format": console_formatter},
                  file_fmt_id: {"format": path_formatter},
                  error_fmt_id: {"format": Constants.LOG_FMT_ERR}
                  }

    handlers = {console_handler_id: console_handler_d,
                file_handler_id: file_handler_d,
                stderr_handler_id: stderr_handler_d}

    loggers = {"console": _to_handler_d([console_handler_id], console_level),
               "custom_file": _to_handler_d([file_handler_id], path_level),
               "stderr_err": _to_handler_d([stderr_handler_id], logging.ERROR)
               }

    d = {'version': 1,
         'disable_existing_loggers': False,  # this fixes the problem
         'formatters': formatters,
         'handlers': handlers,
         'loggers': loggers,
         'root': {'handlers': list(handlers.keys()), 'level': logging.DEBUG}
         }

    # print pprint.pformat(d)
    return d


def _setup_logging_config_d(d):
    logging.config.dictConfig(d)
    logging.Formatter.converter = time.gmtime
    return d


def setup_logger(file_name_or_none, level, formatter=Constants.LOG_FMT_FULL):
    """

    :param file_name_or_none: Path to log file, None will default to stdout
    :param level: logging.LEVEL of
    :param formatter: Log Formatting string
    """
    d = _get_default_logging_config_dict(level, file_name_or_none, formatter)
    return _setup_logging_config_d(d)


def setup_console_and_file_logger(
        stdout_level, stdout_formatter, path, path_level, path_formatter):
    d = _get_console_and_file_logging_config_dict(
        stdout_level, stdout_formatter, path, path_level, path_formatter)
    return _setup_logging_config_d(d)


def setup_log(alog,
              level=logging.INFO,
              file_name=None,
              log_filter=None,
              str_formatter=Constants.LOG_FMT_FULL):
    """Core Util to setup log handler

    :param alog: a log instance
    :param level: (int) Level of logging debug
    :param file_name: (str, None) if None, stdout is used, str write to file
    :param log_filter: (LogFilter, None)
    :param str_formatter: (str) log formatting str

    .. warning:: THIS NEEDS TO BE DEPRECATED
    """
    setup_logger(file_name, level, formatter=str_formatter)

    # FIXME. Keeping the interface, but the specific log instance isn't used,
    # the python logging setup mutates global state
    if log_filter is not None:
        alog.warn("log_filter kw is no longer supported")

    return alog


def get_parsed_args_log_level(pargs, default_level=logging.INFO):
    """
    Utility for handling logging setup flexibly in a variety of use cases,
    assuming standard command-line arguments.

    :param pargs: argparse namespace or equivalent
    :param default_level: logging level to use if the parsed arguments do not
                          specify one
    """
    level = default_level
    if isinstance(level, str):
        level = logging.getLevelName(level)
    verbosity = getattr(pargs, 'verbosity', None)
    if verbosity is not None and verbosity > 0:
        if verbosity >= 2:
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


def log_traceback(alog, ex, ex_traceback):
    """
    Log a python traceback in the log file

    :param ex: python Exception instance
    :param ex_traceback: exception traceback


    Example Usage (assuming you have a log instance in your scope)

    :Example:

    >>> value = 0
    >>> try:
    >>>    1 / value
    >>> except Exception as e:
    >>>    msg = "{i} failed validation. {e}".format(i=value, e=e)
    >>>    log.error(msg)
    >>>    _, _, ex_traceback = sys.exc_info()
    >>>    log_traceback(log, e, ex_traceback)

    """

    tb_lines = traceback.format_exception(ex.__class__, ex, ex_traceback)
    tb_text = ''.join(tb_lines)
    alog.error(tb_text)


def validate_type_or_raise(instance, type_or_types, error_prefix=None):
    _d = dict(t=instance, x=type(instance), v=instance)
    e = error_prefix if error_prefix is not None else ""
    msg = e + "Expected type {t}. Got type {x} for {v}".format(**_d)
    if not isinstance(instance, type_or_types):
        raise TypeError(msg)
    else:
        return instance


def _simple_validate_type(atype, instance):
    return validate_type_or_raise(instance, atype)


_is_argparser_instance = functools.partial(
    _simple_validate_type, argparse.ArgumentParser)


def is_argparser_instance(func):
    @functools.wraps
    def wrapper(*args, **kwargs):
        _is_argparser_instance(args[0])
        return func(*args, **kwargs)
    return wrapper


def compose(*funcs):
    """
    Functional composition of a non-empty list

    [f, g, h] will be f(g(h(x)))

    :Example:

    >>> f = lambda x: x * x
    >>> g = lambda x: x + 1
    >>> h = lambda x: x * 2
    >>> funcs = [f, g, h]
    >>> fgh = compose(*funcs)
    >>> fgh(3) # 49
    >>> compose(f, g, h)(3)

    """
    if not funcs:
        raise ValueError("Compose only supports non-empty lists")
    for func in funcs:
        if not isinstance(func, (types.BuiltinMethodType,
                                 functools.partial,
                                 types.MethodType,
                                 types.BuiltinFunctionType,
                                 types.FunctionType)):
            raise TypeError("Only Function types are supported")

    def compose_two(f, g):
        def c(x):
            return f(g(x))
        return c
    return functools.reduce(compose_two, funcs)


def which(exe_str):
    """walk the current PATH for exe_str to get the absolute path of the exe

    :param exe_str: Executable name

    :rtype: str | None
    :returns Absolute path to the executable or None if the exe is not found
    """
    paths = os.environ.get('PATH', None)
    resolved_exe = None

    if paths is None:
        # log warning
        msg = "PATH env var is not defined."
        log.error(msg)
        return resolved_exe

    for path in paths.split(":"):
        exe_path = os.path.join(path, exe_str)
        # print exe_path
        if os.path.exists(exe_path):
            resolved_exe = exe_path
            break

    # log.debug("Resolved cmd {e} to {x}".format(e=exe_str, x=resolved_exe))
    return resolved_exe


def which_or_raise(cmd):
    """Find exe in path or raise ExternalCommandNotFoundError"""
    resolved_cmd = which(cmd)
    if resolved_cmd is None:
        raise ExternalCommandNotFoundError(
            "Unable to find required cmd '{c}'".format(c=cmd))
    else:
        return resolved_cmd


class Singleton(type):

    """
    General Purpose singleton class

    Usage:

    >>> class MyClass:
    >>>     __metaclass__ = Singleton
    >>>     def __init__(self):
    >>>         self.name = 'name'

    """

    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        cls.instance = None

    def __call__(cls, *args, **kw):
        if cls.instance is None:
            cls.instance = super().__call__(*args)
        return cls.instance


def nfs_exists_check(ff):
    """
    Central place for all NFS hackery

    Return whether a file or a dir ff exists or not.
    Call listdir() instead of os.path.exists() to eliminate NFS errors.

    Added try/catch black hole exception cases to help trigger an NFS refresh

    :rtype bool:
    """
    try:
        # All we really need is opendir(), but listdir() is usually fast.
        os.listdir(os.path.dirname(os.path.realpath(ff)))
        # But is it a file or a directory? We do not know until it actually
        # exists.
        if os.path.exists(ff):
            return True
        # Might be a directory, so refresh itself too.
        # Not sure this is necessary, since we already ran this on parent,
        # but it cannot hurt.
        os.listdir(os.path.realpath(ff))
        if os.path.exists(ff):
            return True
    except OSError:
        pass

    # The rest is probably unnecessary, but it cannot hurt.

    # try to trigger refresh for File case
    try:
        f = open(ff, 'r')
        f.close()
    except Exception:
        pass

    # try to trigger refresh for Directory case
    try:
        _ = os.stat(ff)
        _ = os.listdir(ff)
    except Exception:
        pass

    # Call externally
    # this is taken from Yuan
    cmd = "ls %s" % ff
    rcode = 1
    try:
        p = subprocess.Popen([cmd], shell=True)
        rcode = p.wait()
    except Exception:
        pass

    return rcode == 0


def nfs_refresh(path, ntimes=3, sleep_time=1.0):
    while True:
        if nfs_exists_check(path):
            return True
        ntimes -= 1
        if ntimes <= 0:
            break
        time.sleep(sleep_time)
    log.warn("NFS refresh failed. unable to resolve {p}".format(p=path))
    return False


@contextmanager
def ignored(*exceptions):
    try:
        yield
    except exceptions:
        pass


def get_dataset_metadata(path):
    """
    Returns DataSetMeta data or raises ValueError if dataset XML is missing
    the required UniqueId and MetaType values.

    :param path: Path to DataSet XML
    :raises: ValueError
    :return: DataSetMetaData
    """
    uuid = mt = name = None
    for event, element in ET.iterparse(path, events=("start",)):
        uuid = element.get("UniqueId")
        mt = element.get("MetaType")
        name = element.get("Name")
        break
    else:
        raise ValueError(
            'Did not find events=("start",) in XML path={}'.format(path))
    if mt in FileTypes.ALL_DATASET_TYPES().keys():
        return DataSetMetaData(uuid, mt, name)
    else:
        raise ValueError("Unsupported dataset type '{t}'".format(t=mt))


def get_dataset_metadata_or_none(path):
    """
    Returns DataSetMeta data, else None if the file doesn't exist or a
    processing of the XML raises.

    :param path: Path to DataSet XML
    :return: DataSetMetaData or None
    """
    try:
        return get_dataset_metadata(path)
    except Exception:
        return None


def is_dataset(path):
    """peek into the XML to get the MetaType and verify that it's a valid dataset

    :param path: Path to DataSet XML
    """
    return get_dataset_metadata_or_none(path) is not None


def walker(root_dir, file_filter_func):
    """
    Walk the file sytem and filter by the supplied filter function.

    Filter function F(path) -> bool
    """
    for root, dnames, fnames in os.walk(root_dir):
        for fname in fnames:
            path = os.path.join(root, fname)
            if file_filter_func(path):
                yield path


def pool_map(func, args, nproc):
    """
    Wrapper for calling a function in parallel using the multiprocessing
    module and blocking until results are available.
    """
    nargs = len(args)
    computed_nproc = min(nargs, nproc, multiprocessing.cpu_count())
    if computed_nproc > 1:
        log.debug("Running on %d processors", computed_nproc)
        pool = multiprocessing.Pool(processes=computed_nproc)
        result = pool.map(func, args)  # TODO try map_async instead
        pool.close()
        pool.join()
    else:
        log.debug("computed_nproc=1, running serially")
        result = list(map(func, args))
    return result
