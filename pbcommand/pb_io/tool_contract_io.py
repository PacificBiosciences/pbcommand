"""IO Layer for creating models from files"""
import json
import logging
import datetime

import pbcommand
from pbcommand.models.tool_contract import (ToolDriver,
                                            ToolContractTask,
                                            ToolContract,
                                            ResolvedToolContractTask,
                                            ResolvedToolContract,
                                            InputFileType,
                                            OutputFileType)

log = logging.getLogger(__name__)

__all__ = ['load_resolved_tool_contract_from',
           'load_tool_contract_from',
           'write_tool_contract',
           'write_resolved_tool_contract']


class Constants(object):
    TOOL_ID = "tool_contract_id"
    TOOL = "tool_contract"
    TOOL_TYPE = "task_type"

    RTOOL_ID = "resolved_tool_contract"


class MalformedToolContractError(ValueError):
    pass


class MalformedResolvedToolContractError(ValueError):
    pass


def load_or_raise(ex_type):
    def loader_wrap(func):
        def _wrapper(path):
            msg = "Failed to load {p}".format(p=path)
            try:
                return func(path)
            except Exception as e:
                msg = msg + "{e} {m}".format(m=e.message, e=e)
                log.error(msg, exc_info=True)
                raise ex_type(msg)
        return _wrapper
    return loader_wrap


def resolved_tool_contract_from_d(d):
    """Convert a dict to Resolved Tool Contract"""

    driver_exe = d['driver']['exe']
    driver_env = d['driver'].get('env', {})
    driver = ToolDriver(driver_exe, env=driver_env)

    def _to_a(x):
        return x.encode('ascii', 'ignore')

    def _get(attr_name):
        return d[Constants.TOOL][attr_name]

    def _get_ascii(x_):
        return _to_a(_get(x_))

    tool_contract_id = _get_ascii(Constants.TOOL_ID)
    tool_type = _get_ascii(Constants.TOOL_TYPE)
    # list of strings
    input_files = [_to_a(x) for x in _get("input_files")]
    # list of strings
    output_files = [_to_a(x) for x in _get("output_files")]

    tool_options = _get("options")
    # int
    nproc = _get("nproc")
    resource_types = _get("resources")

    resolved_tool_task = ResolvedToolContractTask(tool_contract_id, tool_type,
                                                  input_files, output_files,
                                                  tool_options, nproc, resource_types)

    rtc = ResolvedToolContract(resolved_tool_task, driver)
    return rtc


@load_or_raise(MalformedResolvedToolContractError)
def load_resolved_tool_contract_from(path):
    with open(path, 'r') as f:
        d = json.loads(f.read())

    return resolved_tool_contract_from_d(d)


def tool_contract_from_d(d):
    """Load tool contract from dict"""
    def _to_a(x):
        return x.encode('ascii', 'ignore')

    def _get(x_):
        if x_ not in d[Constants.TOOL]:
            raise MalformedToolContractError("Unable to find key '{x}'".format(x=x_))
        return d[Constants.TOOL][x_]

    def _get_ascii(x_):
        return _get(x_).encode('ascii', 'ignore')

    def _to_in_ft(fd):
        fx = lambda s: _to_a(fd[s])
        return InputFileType(fx("file_type_id"), fx("id"), fx("title"), fx("description"))

    def _to_out_ft(fd):
        fx = lambda s: _to_a(fd[s])
        return OutputFileType(fx("file_type_id"), fx("id"), fx("title"), fx("description"), fx("default_name"))

    task_id = _to_a(d[Constants.TOOL_ID])
    # FIXME
    display_name = "Display Name"
    version = _to_a(d["version"])
    description = "Description"
    task_type = _get_ascii(Constants.TOOL_TYPE)
    input_types = [_to_in_ft(x) for x in _get("input_types")]
    output_types = [_to_out_ft(x) for x in _get("output_types")]
    tool_options = _get("schema_options")
    nproc = _get("nproc")
    resource_types = _get("resource_types")

    # Add this in version 2
    output_file_names = []
    mutable_files = []

    driver = ToolDriver(d['driver']['exe'], env=d['driver']['env'])
    tool_task = ToolContractTask(task_id, display_name, description, version,
                                 task_type, input_types,
                                 output_types,
                                 tool_options, nproc, resource_types)

    t = ToolContract(tool_task, driver)
    return t


@load_or_raise(MalformedToolContractError)
def load_tool_contract_from(path):
    with open(path, 'r') as f:
        d = json.loads(f.read())
    return tool_contract_from_d(d)


def _write_json(s, output_file):
    with open(output_file, 'w') as f:
        f.write(json.dumps(s))
    return s


def write_tool_contract(p, output_json_file):
    """
    Write a Tool Contract from a PbParser instance

    :param p:
    :type p: pbcommand.models.parser.PbParser
    :param output_json_file:
    :return:
    """
    return _write_json(p.to_contract(), output_json_file)


def write_resolved_tool_contract(rtc, output_json_file):
    """

    :param rtc:
    :type rtc: ResolvedToolContract
    :param output_json_file:
    :return:
    """
    created_at = datetime.datetime.now()
    comment = "Created by pbcommand v{v} at {d}".format(v=pbcommand.get_version(), d=created_at.isoformat())

    # FIXME. there's a lot of assumptions here.
    tc = dict(input_files=rtc.task.input_files,
              output_files=rtc.task.output_files,
              task_type=rtc.task.task_type,
              tool_contract_id=rtc.task.task_id,
              nproc=rtc.task.nproc,
              resources=rtc.task.resources,
              options=rtc.task.options,
              _comment=comment)

    d = dict(tool_contract=tc,
             driver=rtc.driver.to_dict())

    return _write_json(d, output_json_file)
