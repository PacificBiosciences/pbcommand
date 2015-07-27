"""Driver for creating a Resolved Tool Contract from a Tool Contract"""

import logging
import os

from pbcommand.models.common import SymbolTypes, REGISTERED_FILE_TYPES
from pbcommand.models.tool_contract import (ResolvedToolContract,
                                            ToolContract,
                                            ResolvedToolContractTask)

log = logging.getLogger(__name__)


class ToolContractError(BaseException):
    pass


def _resolve_nproc(nproc_int_or_symbol, max_nproc):
    if isinstance(nproc_int_or_symbol, int):
        return min(nproc_int_or_symbol, max_nproc)
    elif nproc_int_or_symbol == SymbolTypes.MAX_NPROC:
        return max_nproc
    else:
        raise TypeError("unsupported type for {s} '{t}".format(t=nproc_int_or_symbol,
                                                               s=SymbolTypes.MAX_NPROC))


def _resolve_options(tool_contract, tool_options):
    resolved_options = {}

    # These probably exist somewhere else, feel free to replace:
    type_map = {'integer': int,
                'object': object,
                'boolean': bool,
                'number': (int, float),
                'string': basestring}

    # Get and Validate resolved value.
    # TODO. None support should be removed.
    for option in tool_contract.task.options:
        for optid in option['required']:
            exp_type = option['properties'][optid]['type']
            value = tool_options.get(optid, option['properties'][optid]['default'])

            if not isinstance(value, type_map[exp_type]):
                raise ToolContractError("Incompatible option types. Supplied "
                                        "{i}. Expected {t}".format(
                                            i=type(value),
                                            t=exp_type))
            resolved_options[optid] = value

    return resolved_options


def resolve_tool_contract(tool_contract, input_files, root_output_dir, root_tmp_dir, max_nproc, tool_options):
    """
    Convert a ToolContract into a Resolved Tool Contract.


    :param tool_contract: Tool Contract interface
    :param input_files: List of input files (must be consistent with the tool contract input file list (types are not enforced)

    :param max_nproc: Max number of processors
    :param tool_options: dict of overridden options

    :type input_files: list[String]
    :type max_nproc: int

    :type tool_contract: ToolContract
    :type tool_options: dict

    :rtype: ResolvedToolContract
    :return: A Resolved tool contract
    """
    if len(input_files) != len(tool_contract.task.input_file_types):
        _d = dict(i=input_files, t=tool_contract.task.input_file_types)
        raise ToolContractError("Incompatible input types. Supplied {i}. Expected file types {t}".format(**_d))

    # Need to clarify how FileTypes defined in the globally registry are required,
    # or not required.

    # need something smarter here. If the file already exists, raise or
    # decide to pick a new name.
    def to_out_file(file_type, file_info):
        if not file_info.default_name:
            base_name = ".".join([file_type.base_name, file_type.ext])
            return os.path.join(root_output_dir, base_name)
        elif isinstance(file_info.default_name, tuple):
            base, ext = file_info.default_name
            return os.path.join(root_output_dir, ".".join([base, ext]))
        else: # XXX should get rid of this eventually
            return os.path.join(root_output_dir, file_info.default_name)

    output_files = [to_out_file(REGISTERED_FILE_TYPES[f.file_type_id], f) for f in tool_contract.task.output_file_types]

    resolved_options = _resolve_options(tool_contract, tool_options)

    nproc = _resolve_nproc(tool_contract.task.nproc, max_nproc)

    resources = []

    log.warn("Resolved options not supported yet.")
    log.warn("Resolved resources not support yet.")
    task = ResolvedToolContractTask(tool_contract.task.task_id,
                                    tool_contract.task.task_type,
                                    input_files,
                                    output_files,
                                    resolved_options,
                                    nproc,
                                    resources)

    return ResolvedToolContract(task, tool_contract.driver)
