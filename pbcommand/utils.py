"""Utils for common funcs, such as setting up a log, composing functions."""
import functools
import os
import sys
import logging
import argparse
import traceback
import time
import types

log = logging.getLogger(__name__)


def setup_log(alog, level=logging.INFO, file_name=None, log_filter=None,
              str_formatter='[%(levelname)s] %(asctime)-15sZ [%(name)s %(funcName)s %(lineno)d] %(message)s'):
    """Core Util to setup log handler

    :param alog: a log instance
    :param level: (int) Level of logging debug
    :param file_name: (str, None) if None, stdout is used, str write to file
    :param log_filter: (LogFilter, None)
    :param str_formatter: (str) log formatting str
    """
    logging.Formatter.converter = time.gmtime

    alog.setLevel(logging.DEBUG)
    if file_name is None:
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(file_name)
    formatter = logging.Formatter(str_formatter)
    handler.setFormatter(formatter)
    handler.setLevel(level)
    if log_filter:
        handler.addFilter(log_filter)
    alog.addHandler(handler)

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
    state = None

    if paths is None:
        # log warning
        msg = "PATH env var is not defined."
        log.error(msg)
        return state

    for path in paths.split(":"):
        exe_path = os.path.join(path, exe_str)
        # print exe_path
        if os.path.exists(exe_path):
            state = exe_path
            break

    return state



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
