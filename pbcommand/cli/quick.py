import argparse
import json
import logging
import os
import sys

from collections import namedtuple
import time

import pbcommand
from .core import get_default_argparser
from pbcommand.common_options import add_base_options, add_common_options

from pbcommand.models import (ToolContractTask, ToolContract,
                              InputFileType, OutputFileType, FileType)
from pbcommand.models.parser import (to_option_schema, JsonSchemaTypes)
from pbcommand.models.tool_contract import ToolDriver
from pbcommand.pb_io import (load_resolved_tool_contract_from,
                             write_tool_contract)
from pbcommand.utils import setup_log, setup_logger

log = logging.getLogger(__name__)

__all__ = ['registry_builder', 'registry_runner', 'Registry']


class Constants(object):
    RTC_DRIVER = 'run-rtc'


QuickOpt = namedtuple("QuickOpt", "value name description")


def _example_main(*args, **kwargs):
    log.info("Running example main with {a} kw:{k}".format(a=args, k=kwargs))
    return 0


def _file_type_to_input_file_type(file_type, index):
    fid = "_".join([file_type.file_type_id, str(index)])
    return InputFileType(file_type.file_type_id,
                         "Label " + fid,
                         repr(file_type),
                         "description for {f}".format(f=fid))


def _file_type_to_output_file_type(file_type, index):
    fid = "_".join([file_type.file_type_id, str(index)])
    return OutputFileType(file_type.file_type_id,
                          "Label " + fid,
                          repr(file_type),
                          "description for {f}".format(f=file_type),
                          file_type.default_name)


def __convert_to_option(jtype, namespace, key, value, name=None, description=None):
    """Convert to Option dict

    This really should have been a concrete type, at least a namedtuple
    """
    opt_id = ".".join([namespace, 'task_options', key])
    name = "Option {n}".format(n=key) if name is None else name
    desc = "Option {n} description".format(n=key) if description is None else description
    opt = to_option_schema(opt_id, jtype, name, desc, value)
    return opt


def _convert_to_option(namespace, key, value, name=None, description=None):
    if isinstance(value, basestring):
        opt = __convert_to_option(JsonSchemaTypes.STR, namespace, key, value, name=name, description=description)
    elif isinstance(value, bool):
        opt = __convert_to_option(JsonSchemaTypes.BOOL, namespace, key, value, name=name, description=description)
    elif isinstance(value, int):
        opt = __convert_to_option(JsonSchemaTypes.INT, namespace, key, value, name=name, description=description)
    elif isinstance(value, float):
        opt = __convert_to_option(JsonSchemaTypes.NUM, namespace, key, value, name=name, description=description)
    else:
        raise TypeError("Unsupported option {k} type. {t} ".format(k=key, t=type(value)))

    return opt


def _convert_quick_option(namespace, key, quick_opt):
    """:type quick_opt: QuickOpt"""
    return _convert_to_option(namespace, key, quick_opt.value,
                              name=quick_opt.name,
                              description=quick_opt.description)


def _to_list(x):
    if isinstance(x, (list, tuple)):
        return x
    else:
        return [x]


def _transform_output_ftype(x, i):
    if isinstance(x, FileType):
        return _file_type_to_output_file_type(x, i)
    elif isinstance(x, OutputFileType):
        return x
    else:
        raise TypeError("Unsupported type {t} value {x}".format(x=x, t=type(x)))


def _convert_to_raw_option(namespace, key, value_or_quick_opt):
    if isinstance(value_or_quick_opt, QuickOpt):
        return _convert_quick_option(namespace, key, value_or_quick_opt)
    else:
        # 'raw' opt was provide with a primitive type
        return _convert_to_option(namespace, key, value_or_quick_opt)


class Registry(object):

    def __init__(self, tool_namespace, driver_base):
        self.namespace = tool_namespace
        self.driver_base = driver_base
        # id -> func(rtc)
        self.rtc_runners = {}

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, n=self.namespace,
                  d=self.driver_base, t=len(self.rtc_runners))
        return "<{k} {n} {d} tool-contracts:{t} >".format(**_d)

    def __call__(self, tool_id, version, input_types, output_types, options=None, nproc=1, is_distributed=True):
        def _w(func):
            """

            Task Options are provided as 'naked' non-namespaced values and
            are automatically type detected and converted to a PacBioOption

            """
            # support list or a single value
            itypes = _to_list(input_types)
            otypes = _to_list(output_types)

            global_id = ".".join([self.namespace, 'tasks', tool_id])
            name = "Tool {n}".format(n=tool_id)
            desc = "Quick tool {n} {g}".format(n=tool_id, g=global_id)

            input_file_types = [_file_type_to_input_file_type(ft, i) for i, ft in enumerate(itypes)]
            output_file_types = [_transform_output_ftype(ft, i) for i, ft in enumerate(otypes)]

            if options is None:
                tool_options = []
            else:
                tool_options = [_convert_to_raw_option(self.namespace, key, value) for key, value in options.iteritems()]

            resource_types = []
            task = ToolContractTask(global_id, name, desc, version, is_distributed,
                                    input_file_types, output_file_types, tool_options, nproc, resource_types)
            # trailing space if for 'my-tool --resolved-tool-contract ' /path/to/rtc.json
            driver_exe = " ".join([self.driver_base, Constants.RTC_DRIVER, " "])
            driver = ToolDriver(driver_exe, )
            tc = ToolContract(task, driver)
            self.rtc_runners[tc] = func
        return _w

    def to_summary(self):
        xs = []
        x = xs.append
        x("Registry namespace:{n} driverbase:{d}".format(n=self.namespace, d=self.driver_base))
        for tc, func in self.rtc_runners.iteritems():
            x(str(tc))

        return "\n".join(xs)


def registry_builder(tool_namespace, driver_base):
    r = Registry(tool_namespace, driver_base)
    return r


def _subparser_builder(subparser, name, description, options_func, exe_func):
    p = subparser.add_parser(name, help=description,
                             formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    options_func(p)
    # I strongly dislike this.
    p.set_defaults(func=exe_func)
    return p


def _add_run_rtc_options(default_log_level=logging.INFO):
    def _wrapper(p):
        add_common_options(p, default_level=default_log_level)
        p.add_argument('rtc_path', type=str, help="Path to resolved tool contract")
        return p
    return _wrapper


def _add_emit_all_tcs_options(p):
    p.add_argument('-o', '--output_dir', type=str, default=os.getcwd(),
                   help='Emit all Tool Contracts to output directory')
    return p


def _add_emit_tc_options(p):
    p.add_argument('tc_id', type=str, help="Tool Contract Id")
    return p


def __args_summary_runner(registry):
    def _w(args):
        log.info("Registry {r}".format(r=registry))
        log.info("\n" + registry.to_summary())
        print registry.to_summary()
        return 0
    return _w


def __args_rtc_runner(registry, default_log_level):
    def _w(args):

        started_at = time.time()

        def run_time():
            return time.time() - started_at

        def exit_msg(rcode_):
            return "Completed running {r} exitcode {e} in {t:.2f} sec.".format(r=rtc, e=rcode_, t=run_time())

        level = getattr(args, 'log_level', default_log_level)
        is_debug = getattr(args, 'debug', False)
        is_quiet = getattr(args, 'quiet', False)

        if is_debug:
            level = logging.DEBUG

        # quiet trumps debug or the provided log level
        if is_quiet:
            level = logging.ERROR

        setup_logger(None, level=level)

        log.info("Loading pbcommand {v}".format(v=pbcommand.get_version()))
        log.info("Registry {r}".format(r=registry))
        log.info("Setting log-level to {d}".format(d=level))
        log.debug("args {a}".format(a=args))
        log.info("loading RTC from {i}".format(i=args.rtc_path))
        rtc = load_resolved_tool_contract_from(args.rtc_path)
        id_funcs = {t.task.task_id: func for t, func in registry.rtc_runners.iteritems()}
        func = id_funcs.get(rtc.task.task_id, None)
        if func is None:
            rcode = 1
            log.error("Unknown tool contract id '{x}' Registered TC ids {i}".format(x=rtc.task.task_id, i=id_funcs.keys()))
            log.error(exit_msg(rcode))
            return rcode
        else:
            log.info("Running id:{i} Resolved Tool Contract {r}".format(r=rtc, i=rtc.task.task_id))
            log.info("Runner func {f}".format(f=func))
            exit_code = func(rtc)
            if exit_code == 0:
                log.info(exit_msg(exit_code))
            else:
                log.error(exit_msg(exit_code))

            return exit_code
    return _w


def __args_emit_tc_runner(registry):
    def _w(args):
        log.info("Registry {r}".format(r=registry))
        tc_id = args.tc_id
        log.info("Emitting TC from {i}".format(i=tc_id))
        id_tc = {t.task.task_id: t for t in registry.rtc_runners.keys()}
        log.info(id_tc)
        tc = id_tc.get(tc_id, None)
        if tc is None:
            sys.stderr.write("ERROR. Unable to find tool-contract id {i}".format(i=tc_id))
            return -1
        else:
            print json.dumps(tc.to_dict(), sort_keys=True, indent=4)
            return 0
    return _w


def __args_emit_all_tcs_runner(registry):
    def _w(args):
        log.info("Registry {r}".format(r=registry))
        log.info(registry.to_summary())
        log.info("Emitting TCs to {i}".format(i=args.output_dir))
        tcs = registry.rtc_runners.keys()
        for tc in tcs:
            output_file = os.path.join(args.output_dir, tc.task.task_id + "_tool_contract.json")
            write_tool_contract(tc, output_file)
        return 0
    return _w


def _to_registry_parser(version, description, default_log_level):
    def _f(registry):
        p = get_default_argparser(version, description)
        sp = p.add_subparsers(help='Commands')

        args_summary_runner = __args_summary_runner(registry)
        args_rtc_runner = __args_rtc_runner(registry, default_log_level)
        args_tc_emit = __args_emit_tc_runner(registry)
        args_tcs_emit = __args_emit_all_tcs_runner(registry)

        _subparser_builder(sp, Constants.RTC_DRIVER, "Run Resolved Tool contract", _add_run_rtc_options(default_log_level), args_rtc_runner)
        _subparser_builder(sp, 'emit-tool-contracts', "Emit all Tool contracts to output-dir", _add_emit_all_tcs_options, args_tcs_emit)
        _subparser_builder(sp, 'emit-tool-contract', "Emit a single tool contract by id", _add_emit_tc_options, args_tc_emit)
        _subparser_builder(sp, 'summary', "Summary of Tool Contracts", lambda x: x, args_summary_runner)
        return p
    return _f


def registry_runner(registry, argv, default_log_level=logging.INFO):
    """Runs a registry

    1. Manually build an argparser that has

    For running:

    my_tool run-rtc /path/to/resolved-tool-contract.json

    For emitting:

    my-tool emit-tool-contracts /path/to/output-dir
    my-tool emit-tool-contract global_id

    :type registry: Registry
    """
    f = _to_registry_parser('0.1.1', "Multi-quick-tool-runner for {r}".format(r=registry.namespace), default_log_level)
    p = f(registry)
    args = p.parse_args(argv)
    # The logger needs to be setup only in specific subparsers. Some commands
    # are using the stdout as a non logging model
    return_code = args.func(args)
    return return_code
