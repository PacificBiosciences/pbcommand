"""IO Layer for creating models from files"""
import json
import logging

from pbcommand.models.tool_contract import (ToolDriver,
                                            ToolContractTask,
                                            ToolContract,
                                            ResolvedToolContractTask,
                                            ResolvedToolContract)

log = logging.getLogger(__name__)


class Constants(object):
    TOOL_ID = "tool_contract_id"
    TOOL = "tool_contract"
    TOOL_TYPE = "tool_type"


def load_resolved_tool_contract_from(path):
    with open(path, 'r') as f:
        d = json.loads(f.read())

    driver_exe = d['driver']['exe']
    driver_env = d['driver']['env']
    driver = ToolDriver(driver_exe, env=driver_env)

    def _to_a(x):
        return x.encode('ascii', 'ignore')

    def _get(attr_name):
        return d[Constants.TOOL][attr_name]

    def _get_ascii(x_):
        return _to_a(_get(x_))

    tool_contract_id = _get_ascii(Constants.TOOL_ID)
    tool_type = _get_ascii(Constants.TOOL_TYPE)
    input_files = [_to_a(x) for x in _get("input_files")]
    output_files = [_to_a(x) for x in _get("output_files")]
    tool_options = _get("options")
    nproc = _get("nproc")
    resource_types = _get("resources")
    resolved_tool_task = ResolvedToolContractTask(tool_contract_id, tool_type, input_files, output_files, tool_options, nproc, resource_types)

    dm = ResolvedToolContract(resolved_tool_task, driver)
    return dm


def load_tool_contract_from(path):

    with open(path, 'r') as f:
        d = json.loads(f.read())

    def _to_a(x):
        return x.encode('ascii', 'ignore')

    def _get(x_):
        return d[Constants.TOOL][x_]

    def _get_ascii(x_):
        return _get(x_).encode('ascii', 'ignore')

    task_id = _get_ascii(Constants.TOOL_ID)
    display_name = _get_ascii("name")
    version = _get_ascii("version")
    description = _get_ascii("description")
    task_type = _get_ascii(Constants.TOOL_TYPE)
    input_types = [_to_a(x) for x in _get("input_types")]
    output_types = [_to_a(x) for x in _get("output_types")]
    tool_options = _get("schema_options")
    nproc = _get("nproc")
    resource_types = _get("resource_types")

    # Add this in version 2
    output_file_names = []
    mutable_files = []

    driver = ToolDriver(d['driver']['exe'], env=d['driver']['env'])
    tool_task = ToolContractTask(task_id, display_name, description, version, task_type, input_types, output_types, tool_options, nproc, resource_types)

    t = ToolContract(tool_task, driver)
    return t