"""IO Layer for creating models from files"""
import json
import logging
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

import pbcommand

from pbcommand.schemas import RTC_SCHEMA, TC_SCHEMA
from pbcommand.models import (TaskTypes,
                              GatherToolContractTask,
                              ScatterToolContractTask,
                              MalformedToolContractError,
                              MalformedResolvedToolContractError,
                              validate_tool_contract)

from pbcommand.models.tool_contract import (ToolDriver,
                                            ToolContractTask,
                                            ToolContract,
                                            ResolvedToolContractTask,
                                            ResolvedToolContract,
                                            InputFileType,
                                            OutputFileType,
                                            ResolvedScatteredToolContractTask,
                                            ResolvedGatherToolContractTask,
                                            ToolContractResolvedResource)

log = logging.getLogger(__name__)

__all__ = ['load_resolved_tool_contract_from',
           'load_tool_contract_from',
           'write_tool_contract',
           'write_resolved_tool_contract']


class Constants(object):
    TOOL_ID = "tool_contract_id"
    TOOL = "tool_contract"
    TOOL_TYPE = "task_type"
    IS_DIST = 'is_distributed'

    # Serialization Format
    SERIALIZATION = 'serialization'

    # Scatter TC, mirrors the nproc key in the JSON
    NCHUNKS = "nchunks"

    RTOOL = "resolved_tool_contract"
    # Used in Scattering/Chunking tasks to
    # produce chunks with specific $chunk_keys
    CHUNK_KEYS = "chunk_keys"
    MAX_NCHUNKS = 'max_nchunks'

    # Used in Gather Tasks
    GATHER_CHUNK_KEY = 'chunk_key'


def load_or_raise(ex_type):
    def loader_wrap(func):
        def _wrapper(path):
            msg = "Failed to load {p}".format(p=path)
            try:
                return func(path)
            except Exception as e:
                msg = msg + " {e} {m}".format(m=e.message, e=e)
                log.error(msg, exc_info=True)
                raise ex_type(msg)
        return _wrapper
    return loader_wrap


def __driver_from_d(d):
    driver_exe = d['driver']['exe']
    driver_env = d['driver'].get('env', {})
    serialization = d['driver'].get(Constants.SERIALIZATION, 'json')
    return ToolDriver(driver_exe, env=driver_env, serialization=serialization)


def __core_resolved_tool_contract_task_from_d(d):
    def _to_a(x):
        return x.encode('ascii', 'ignore')

    def _get(attr_name):
        return d[Constants.RTOOL][attr_name]

    def _get_ascii(x_):
        return _to_a(_get(x_))

    tool_contract_id = _get_ascii(Constants.TOOL_ID)
    tool_type = _get_ascii(Constants.TOOL_TYPE)
    is_distributed = _get(Constants.IS_DIST)
    # list of strings
    input_files = [_to_a(x) for x in _get("input_files")]
    # list of strings
    output_files = [_to_a(x) for x in _get("output_files")]

    tool_options = _get("options")
    # int
    nproc = _get("nproc")

    resource_types = [ToolContractResolvedResource.from_d(dx) for dx in _get("resources")]

    return tool_contract_id, is_distributed, input_files, output_files, tool_options, nproc, resource_types


def __to_rtc_from_d(d):
    def _wrapper(task):
        driver = __driver_from_d(d)
        rtc = ResolvedToolContract(task, driver)
        return rtc
    return _wrapper


def _standard_resolved_tool_contract_from_d(d):
    """Load a 'Standard' CLI task type"""

    tool_contract_id, is_distributed, input_files, output_files, tool_options, nproc, resource_types = __core_resolved_tool_contract_task_from_d(d)

    task = ResolvedToolContractTask(tool_contract_id, is_distributed,
                                    input_files, output_files,
                                    tool_options, nproc, resource_types)
    return __to_rtc_from_d(d)(task)


def _scatter_resolved_tool_contract_from_d(d):
    """Load a Gathered Tool Contract """
    tool_contract_id, is_distributed, input_files, output_files, tool_options, nproc, resource_types = __core_resolved_tool_contract_task_from_d(d)
    max_nchunks = d[Constants.RTOOL][Constants.MAX_NCHUNKS]
    chunk_keys = d[Constants.RTOOL][Constants.CHUNK_KEYS]
    task = ResolvedScatteredToolContractTask(tool_contract_id, is_distributed, input_files, output_files, tool_options, nproc, resource_types, max_nchunks, chunk_keys)

    return __to_rtc_from_d(d)(task)


def _gather_resolved_tool_contract_from_d(d):
    tool_contract_id, is_distributed, input_files, output_files, tool_options, nproc, resource_types = __core_resolved_tool_contract_task_from_d(d)

    chunk_key = d[Constants.RTOOL][Constants.GATHER_CHUNK_KEY]
    task = ResolvedGatherToolContractTask(tool_contract_id, is_distributed,
                                      input_files, output_files,
                                      tool_options, nproc, resource_types, chunk_key)
    return __to_rtc_from_d(d)(task)


def resolved_tool_contract_from_d(d):
    """Convert a dict to Resolved Tool Contract"""

    def _to_a(x):
        return x.encode('ascii', 'ignore')

    def _get(attr_name):
        return d[Constants.RTOOL][attr_name]

    def _get_ascii(x_):
        return _to_a(_get(x_))

    tool_type = _get_ascii(Constants.TOOL_TYPE)

    dispatch_funcs = {TaskTypes.STANDARD: _standard_resolved_tool_contract_from_d,
                      TaskTypes.GATHERED: _gather_resolved_tool_contract_from_d,
                      TaskTypes.SCATTERED: _scatter_resolved_tool_contract_from_d}

    if tool_type in dispatch_funcs:
        return dispatch_funcs[tool_type](d)
    else:
        raise ValueError("Unsupported task type '{x}' Supported task types {t}".format(x=tool_type, t=dispatch_funcs.keys()))


def json_path_or_d(value):
    if isinstance(value, dict):
        return value
    elif isinstance(value, basestring):
        with open(value, 'r') as f:
            d = json.loads(f.read())
        return d
    else:
        raise ValueError("Unsupported value. Expected dict, or string")


def _json_path_or_d(func):
    def _wrapper(value):
        return func(json_path_or_d(value))
    return _wrapper


@load_or_raise(MalformedResolvedToolContractError)
@_json_path_or_d
def load_resolved_tool_contract_from(path_or_d):
    return resolved_tool_contract_from_d(path_or_d)


@_json_path_or_d
def __core_tool_contract_task_from(d):
    def _to_a(x):
        return x.encode('ascii', 'ignore')

    def _get(x_):
        if x_ not in d[Constants.TOOL]:
            raise MalformedToolContractError("Unable to find key '{x}'".format(x=x_))
        return d[Constants.TOOL][x_]

    def _get_or(x_, default):
        return d[Constants.TOOL].get(x_, default)

    def _get_ascii(x_):
        return _to_a(_get(x_))

    def _get_ascii_or(x_, default):
        return _to_a(_get_or(x_, default))

    def _to_in_ft(fd):
        fx = lambda s: _to_a(fd[s])
        return InputFileType(fx("file_type_id"), fx("id"), fx("title"), fx("description"))

    def _to_out_ft(fd):
        fx = lambda s: _to_a(fd[s])
        return OutputFileType(fx("file_type_id"), fx("id"), fx("title"), fx("description"), fx("default_name"))

    task_id = _to_a(d[Constants.TOOL_ID])
    display_name = _get_ascii("name")
    version = _to_a(d["version"])
    default_desc = "PacBio Tool {n}".format(n=display_name)
    description = _get_ascii_or("description", default_desc)
    is_distributed = _get(Constants.IS_DIST)

    input_types = [_to_in_ft(x) for x in _get("input_types")]
    output_types = [_to_out_ft(x) for x in _get("output_types")]
    tool_options = _get("schema_options")
    nproc = _get("nproc")
    resource_types = _get("resource_types")
    return task_id, display_name, description, version, is_distributed, input_types, output_types, tool_options, nproc, resource_types


def __to_tc_from_d(d):
    def _wrapper(task):
        driver = __driver_from_d(d)
        tc = ToolContract(task, driver)
        return tc
    return _wrapper


@_json_path_or_d
def _standard_tool_contract_from(path_or_d):
    task_id, display_name, description, version, is_distributed, input_types, output_types, tool_options, nproc, resource_types  = __core_tool_contract_task_from(path_or_d)
    task = ToolContractTask(task_id, display_name, description, version,
                            is_distributed,
                            input_types,
                            output_types,
                            tool_options, nproc, resource_types)
    return __to_tc_from_d(path_or_d)(task)


@_json_path_or_d
def _scattered_tool_contract_from(path_or_d):
    task_id, display_name, description, version, is_distributed, input_types, output_types, tool_options, nproc, resource_types = __core_tool_contract_task_from(path_or_d)

    chunk_keys = path_or_d[Constants.TOOL][Constants.CHUNK_KEYS]
    # int, or SymbolTypes.MAX_NCHUNKS
    nchunks = path_or_d[Constants.TOOL][Constants.NCHUNKS]
    task = ScatterToolContractTask(task_id, display_name, description, version,
                                   is_distributed,
                                   input_types,
                                   output_types,
                                   tool_options, nproc, resource_types, chunk_keys, nchunks)
    return __to_tc_from_d(path_or_d)(task)


@_json_path_or_d
def _gather_tool_contract_from(path_or_d):
    task_id, display_name, description, version, is_distributed, input_types, output_types, tool_options, nproc, resource_types = __core_tool_contract_task_from(path_or_d)
    task = GatherToolContractTask(task_id, display_name, description, version,
                                  is_distributed,
                                  input_types,
                                  output_types,
                                  tool_options, nproc, resource_types)
    return __to_tc_from_d(path_or_d)(task)


@_json_path_or_d
def tool_contract_from_d(d):
    """Load tool contract from dict"""

    task_type = d[Constants.TOOL][Constants.TOOL_TYPE]

    dispatch_funcs = {TaskTypes.SCATTERED: _scattered_tool_contract_from,
                      TaskTypes.GATHERED: _gather_tool_contract_from,
                      TaskTypes.STANDARD: _standard_tool_contract_from}

    if task_type in dispatch_funcs:
        tc = dispatch_funcs[task_type](d)
        return validate_tool_contract(tc)
    else:
        raise ValueError("Unsupported task type {x}".format(x=task_type))


@load_or_raise(MalformedToolContractError)
@_json_path_or_d
def load_tool_contract_from(path_or_d):
    return tool_contract_from_d(path_or_d)


def _write_json(s, output_file):
    with open(output_file, 'w') as f:
        f.write(json.dumps(s, indent=4, sort_keys=True))
    return s


def write_tool_contract(tool_contract, output_json_file):
    """
    Write a Tool Contract

    :type tool_contract: ToolContract
    :param output_json_file:
    :return:
    """
    return _write_json(tool_contract.to_dict(), output_json_file)


def write_resolved_tool_contract(rtc, output_json_file):
    """

    :param rtc:
    :type rtc: ResolvedToolContract
    :param output_json_file:
    :return:
    """
    d = rtc.to_dict()
    return _write_json(d, output_json_file)


def _write_records_to_avro(schema, _d_or_ds, output_file):
    # FIXME. There's only one record being written here,
    # why does this not support a single item
    if isinstance(_d_or_ds, dict):
        _d_or_ds = [_d_or_ds]
    with open(output_file, 'w') as outs:
        with DataFileWriter(outs, DatumWriter(), schema) as writer:
            for record in _d_or_ds:
                writer.append(record)
    log.debug("Write avro file to {p}".format(p=output_file))
    return _d_or_ds


def write_tool_contract_avro(tc, avro_output):
    return _write_records_to_avro(TC_SCHEMA, tc.to_dict(), avro_output)


def write_resolved_tool_contract_avro(rtc, avro_output):
    return _write_records_to_avro(RTC_SCHEMA, rtc.to_dict(), avro_output)
