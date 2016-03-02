"""Utils for common funcs, such as setting up a log, composing functions."""
import functools
import os
import logging
import logging.config
import argparse
import pprint
import traceback
import time
import types
import subprocess
from contextlib import contextmanager
import xml.etree.ElementTree as ET

from pbcommand.models import FileTypes, DataSetMetaData

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())  # suppress the annoying no handlers msg


class Constants(object):
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

_handler_stdout_stream_d = functools.partial(_handler_stream_d, "ext://sys.stdout")
_handler_stderr_stream_d = functools.partial(_handler_stream_d, "ext://sys.stderr")


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

    formatters_d = {fid: {'format': fx} for fid, fx in [(formatter_id, formatter), (error_fmt_id, Constants.LOG_FMT_ERR)]}

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

    #print pprint.pformat(d)
    return d


def _get_console_and_file_logging_config_dict(console_level, console_formatter, path, path_level, path_formatter):
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
         'root': {'handlers': handlers.keys(), 'level': logging.DEBUG}
         }

    #print pprint.pformat(d)
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


def setup_console_and_file_logger(stdout_level, stdout_formatter, path, path_level, path_formatter):
    d = _get_console_and_file_logging_config_dict(stdout_level, stdout_formatter, path, path_level, path_formatter)
    return _setup_logging_config_d(d)


def setup_log(alog,
              level=logging.INFO,
              file_name=None,
              log_filter=None,
              str_formatter=Constants.LOG_FMT_FULL):
    """Core Util to setup log handler

    THIS NEEDS TO BE DEPRECATED

    :param alog: a log instance
    :param level: (int) Level of logging debug
    :param file_name: (str, None) if None, stdout is used, str write to file
    :param log_filter: (LogFilter, None)
    :param str_formatter: (str) log formatting str
    """
    setup_logger(file_name, level, formatter=str_formatter)

    # FIXME. Keeping the interface, but the specific log instance isn't used,
    # the python logging setup mutates global state
    if log_filter is not None:
        alog.warn("log_filter kw is no longer supported")

    return alog


def log_traceback(alog, ex, ex_traceback):
    """
    Log a python traceback in the log file

    :param ex: python Exception instance
    :param ex_traceback: exception traceback


    Example Usage (assuming you have a log instance in your scope)

    try:
        1 / 0
    except Exception as e:
        msg = "{i} failed validation. {e}".format(i=item, e=e)
        log.error(msg)
        _, _, ex_traceback = sys.exc_info()
        log_traceback(log, e, ex_traceback)

    """

    tb_lines = traceback.format_exception(ex.__class__, ex, ex_traceback)
    tb_text = ''.join(tb_lines)
    alog.error(tb_text)


def _simple_validate_type(atype, instance):
    if not isinstance(instance, atype):
        _d = dict(t=atype, x=type(instance), v=instance)
        raise TypeError("Expected type {t}. Got type {x} for {v}".format(**_d))
    return instance

_is_argparser_instance = functools.partial(_simple_validate_type, argparse.ArgumentParser)


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

    fx = compose(f, g, h)

    or

    fx = compose(*[f, g, h])

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
    """walk the exe_str in PATH to get current exe_str.

    If path is found, the full path is returned. Else it returns None.
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
    resolved_cmd = which(cmd)
    if resolved_cmd is None:
        raise ExternalCommandNotFoundError("Unable to find required cmd '{c}'".format(c=cmd))
    else:
        return resolved_cmd


class Singleton(type):

    """
    General Purpose singleton class

    Usage:

    >>> class MyClass(object):
    >>>     __metaclass__ = Singleton
    >>>     def __init__(self):
    >>>         self.name = 'name'

    """

    def __init__(cls, name, bases, dct):
        super(Singleton, cls).__init__(name, bases, dct)
        cls.instance = None

    def __call__(cls, *args, **kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args)
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
        # But is it a file or a directory? We do not know until it actually exists.
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
    Returns DataSetMeta data or raises ValueError, KeyError

    :param path:
    :return:
    """
    f = ET.parse(path).getroot().attrib
    mt = f['MetaType']
    uuid = f['UniqueId']
    if mt in FileTypes.ALL_DATASET_TYPES().keys():
        return DataSetMetaData(uuid, mt)
    else:
        raise ValueError("Unsupported dataset type '{t}'".format(t=mt))


def get_dataset_metadata_or_none(path):
    """
    Returns DataSetMeta data, else None

    :param path:
    :return:
    """
    try:
        return get_dataset_metadata(path)
    except Exception:
        return None


def is_dataset(path):
    """peek into the XML to get the MetaType"""
    return get_dataset_metadata_or_none(path) is not None


def walker(root_dir, file_filter_func):
    """Filter files F(path) -> bool"""
    for root, dnames, fnames in os.walk(root_dir):
        for fname in fnames:
            path = os.path.join(root, fname)
            if file_filter_func(path):
                yield path
