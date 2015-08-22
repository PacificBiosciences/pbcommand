import json
import logging
import os
import sys
from pbcommand.cli import get_default_argparser

from pbcommand.models import (FileTypes, ToolContractTask, ToolContract,
                              InputFileType, OutputFileType, FileType)
from pbcommand.models.parser import (to_option_schema, JsonSchemaTypes)
from pbcommand.models.tool_contract import ToolDriver
from pbcommand.pb_io import (load_resolved_tool_contract_from,
                             write_tool_contract)
from pbcommand.utils import setup_log

log = logging.getLogger(__name__)

__all__ = ['registry_builder', 'registry_runner', 'Registry']


class Constants(object):
    RTC_DRIVER = 'run-rtc'


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


def __convert_to_option(jtype, namespace, key, value):
    opt_id = ".".join([namespace, 'task_options', key])
    name = "Option {n}".format(n=key)
    desc = "Option {n} description".format(n=key)
    opt = to_option_schema(opt_id, jtype, name, desc, value)
    return opt


def _convert_to_option(namespace, key, value):
    if isinstance(value, basestring):
        opt = __convert_to_option(JsonSchemaTypes.STR, namespace, key, value)
    elif isinstance(value, bool):
        opt = __convert_to_option(JsonSchemaTypes.BOOL, namespace, key, value)
    elif isinstance(value, int):
        opt = __convert_to_option(JsonSchemaTypes.INT, namespace, key, value)
    elif isinstance(value, float):
        opt = __convert_to_option(JsonSchemaTypes.NUM, namespace, key, value)
    else:
        raise TypeError("Unsupported option {k} type. {t} ".format(k=key, t=type(value)))

    return opt


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
                tool_options = [_convert_to_option(self.namespace, key, value) for key, value in options.iteritems()]

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
    p = subparser.add_parser(name, help=description)
    options_func(p)
    # I strongly dislike this.
    p.set_defaults(func=exe_func)
    return p


def _add_run_rtc_options(p):
    p.add_argument('rtc_path', type=str, help="Path to resolved tool contract")
    return p


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


def __args_rtc_runner(registry):
    def _w(args):
        # FIXME.
        setup_log(log, level=logging.DEBUG)

        log.info("Registry {r}".format(r=registry))
        log.info("loading RTC from {i}".format(i=args.rtc_path))
        rtc = load_resolved_tool_contract_from(args.rtc_path)
        id_funcs = {t.task.task_id:func for t, func in registry.rtc_runners.iteritems()}
        func = id_funcs.get(rtc.task.task_id, None)
        if func is None:
            sys.stderr.write("ERROR. Unknown tool contract id {x}".format(x=rtc.task.task_id))
            return -1
        else:
            exit_code = func(rtc)
            log.info("Completed running {r} exitcode {e}".format(r=rtc, e=exit_code))
    return _w


def __args_emit_tc_runner(registry):
    def _w(args):
        log.info("Registry {r}".format(r=registry))
        tc_id = args.tc_id
        log.info("Emitting TC from {i}".format(i=tc_id))
        id_tc = {t.task.task_id:t for t in registry.rtc_runners.keys()}
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


def _to_registry_parser(version, description):
    def _f(registry):
        p = get_default_argparser(version, description)
        sp = p.add_subparsers(help='Commands')

        args_summary_runner = __args_summary_runner(registry)
        args_rtc_runner = __args_rtc_runner(registry)
        args_tc_emit = __args_emit_tc_runner(registry)
        args_tcs_emit = __args_emit_all_tcs_runner(registry)

        _subparser_builder(sp, Constants.RTC_DRIVER, "Run Resolved Tool contract", _add_run_rtc_options, args_rtc_runner)
        _subparser_builder(sp, 'emit-tool-contracts', "Emit all Tool contracts to output-dir", _add_emit_all_tcs_options, args_tcs_emit)
        _subparser_builder(sp, 'emit-tool-contract', "Emit a single tool contract by id", _add_emit_tc_options, args_tc_emit)
        _subparser_builder(sp, 'summary', "Summary of Tool Contracts", lambda x: x, args_summary_runner)
        return p
    return _f


def registry_runner(registry, argv):
    """Runs a registry

    1. Manually build an argparser that has

    For running:

    my_tool run-rtc /path/to/resolved-tool-contract.json

    For emitting:

    my-tool emit-tool-contracts /path/to/output-dir
    my-tool emit-tool-contract global_id

    :type registry: Registry
    """
    log.info("Running registry {r} with args {a}".format(r=registry, a=argv))
    f = _to_registry_parser('0.1.0', "Multi-quick-tool-runner for {r}".format(r=registry.namespace))
    p = f(registry)
    args = p.parse_args(argv)
    # need to disable this because some subparsers are emitting to stdout
    # setup_log(log, level=logging.DEBUG)
    return_code = args.func(args)
    return return_code
