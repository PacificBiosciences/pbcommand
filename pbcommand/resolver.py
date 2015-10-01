"""Driver for creating a Resolved Tool Contract from a Tool Contract"""
from collections import defaultdict

import logging
import os
import uuid

from pbcommand.models.common import (SymbolTypes, REGISTERED_FILE_TYPES,
                                     ResourceTypes)
from pbcommand.models.tool_contract import (ResolvedToolContract,
                                            ToolContract,
                                            ResolvedToolContractTask,
                                            ResolvedScatteredToolContractTask,
                                            ResolvedGatherToolContractTask,
                                            ToolContractResolvedResource)

log = logging.getLogger(__name__)


class ToolContractError(BaseException):
    pass


def __resolve_int_or_symbol(symbol_type, symbol_or_int, max_value):
    if isinstance(symbol_or_int, int):
        return min(symbol_or_int, max_value)
    elif symbol_or_int == symbol_type:
        return max_value
    else:
        raise TypeError("unsupported type for {s} '{t}".format(t=symbol_or_int,
                                                               s=symbol_type))


def _resolve_nproc(nproc_int_or_symbol, max_nproc):
    return __resolve_int_or_symbol(SymbolTypes.MAX_NPROC, nproc_int_or_symbol, max_nproc)


def _resolve_max_nchunks(nchunks_or_symbol, max_nchunks):
    return __resolve_int_or_symbol(SymbolTypes.MAX_NCHUNKS, nchunks_or_symbol, max_nchunks)


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


def _resolve_output_file(registry_d, file_type, output_file_type, root_output_dir):
    """
    Resolved the Output File Type

    :type file_type: pbcommand.models.FileType
    :type output_file_type: pbcommand.models.OutputFileType
    :return: Resolved output file name
    """
    def _get_fname(base, ext):
        idx = base, ext
        count = registry_d[idx]
        xs = "" if count == 0 else "-" + str(count)
        registry_d[idx] += 1
        name = "".join([base, xs, ".", ext])
        return os.path.join(root_output_dir, name)

    # FIXME. THIS NEED TO BE FUNDAMENTALLY FIXED and updated to use the spec
    # in the avro schema.
    if isinstance(output_file_type.default_name, basestring):
        a, b = os.path.splitext(output_file_type.default_name)
        return _get_fname(a, b.replace('.', ''))
    elif isinstance(output_file_type.default_name, (list, tuple)):
        base, ext = output_file_type.default_name
        return _get_fname(base, ext)
    else:
        return _get_fname(file_type.base_name, file_type.ext)


def _resolve_resource_types(resources, output_dir, root_tmp_dir):
    resolved_resources = []

    def _add(rt_id, p):
        r = ToolContractResolvedResource(rt_id, p)
        resolved_resources.append(r)
        return r

    def _to_p(x):
        return os.path.join(root_tmp_dir, x)

    def _to_r(prefix, suffix=None):
        u = uuid.uuid4()
        name = "{x}-{u}".format(u=u, x=prefix)
        if suffix is not None:
            name += suffix
        return _to_p(name)

    # The names are not optimal, this would require more config
    for resource in resources:
        if resource == ResourceTypes.TMP_DIR:
            path = _to_r("pb-tmp")
            _add(resource, path)
        elif resource == ResourceTypes.TMP_FILE:
            _add(resource, _to_r("pb-tmp", "-file"))
        elif resource == ResourceTypes.LOG_FILE:
            u = uuid.uuid4()
            name = "{x}-{u}-log".format(u=u, x="pb-tmp")
            path = os.path.join(output_dir, name)
            _add(resource, path)
        else:
            raise ValueError("Unsupported Resource Type {x}".format(x=resource))

    return resolved_resources


def _resolve_output_files(output_file_types, root_output_dir):
    # store the files as {(base, ext): count}
    _outs_registry = defaultdict(lambda : 0)
    return [_resolve_output_file(_outs_registry, REGISTERED_FILE_TYPES[f.file_type_id], f, root_output_dir) for f in output_file_types]


def _resolve_core(tool_contract, input_files, root_output_dir, max_nproc, tool_options, tmp_dir=None):

    if len(input_files) != len(tool_contract.task.input_file_types):
        _d = dict(i=input_files, t=tool_contract.task.input_file_types)
        raise ToolContractError("Incompatible input types. Supplied {i}. Expected file types {t}".format(**_d))

    output_files = _resolve_output_files(tool_contract.task.output_file_types, root_output_dir)

    resolved_options = _resolve_options(tool_contract, tool_options)

    nproc = _resolve_nproc(tool_contract.task.nproc, max_nproc)

    resolved_resources = _resolve_resource_types(tool_contract.task.resources, root_output_dir, tmp_dir)

    return output_files, resolved_options, nproc, resolved_resources


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
    output_files, resolved_options, nproc, resources = _resolve_core(tool_contract, input_files, root_output_dir, max_nproc, tool_options, root_tmp_dir)
    task = ResolvedToolContractTask(tool_contract.task.task_id,
                                    tool_contract.task.is_distributed,
                                    input_files,
                                    output_files,
                                    resolved_options,
                                    nproc,
                                    resources)

    return ResolvedToolContract(task, tool_contract.driver)


def resolve_scatter_tool_contract(tool_contract, input_files, root_output_dir, root_tmp_dir, max_nproc, tool_options, max_nchunks, chunk_keys):
    output_files, resolved_options, nproc, resources = _resolve_core(tool_contract, input_files, root_output_dir, max_nproc, tool_options, tmp_dir=root_tmp_dir)
    resolved_max_chunks = _resolve_max_nchunks(tool_contract.task.max_nchunks, max_nchunks)
    task = ResolvedScatteredToolContractTask(tool_contract.task.task_id,
                                             tool_contract.task.is_distributed,
                                             input_files,
                                             output_files,
                                             resolved_options,
                                             nproc,
                                             resources, resolved_max_chunks, chunk_keys)
    return ResolvedToolContract(task, tool_contract.driver)


def resolve_gather_tool_contract(tool_contract, input_files, root_output_dir, root_tmp_dir, max_nproc, tool_options, chunk_key):
    output_files, resolved_options, nproc, resources = _resolve_core(tool_contract, input_files, root_output_dir, max_nproc, tool_options, tmp_dir=root_tmp_dir)
    task = ResolvedGatherToolContractTask(tool_contract.task.task_id,
                                          tool_contract.task.is_distributed,
                                          input_files,
                                          output_files,
                                          resolved_options,
                                          nproc,
                                          resources, chunk_key)
    return ResolvedToolContract(task, tool_contract.driver)
