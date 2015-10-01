"""Common models for Tool Contract and Resolved Tool Contract


Author: Michael Kocher
"""
import abc

import pbcommand
from pbcommand.models import TaskTypes, ResourceTypes

__version__ = pbcommand.get_version()


class MalformedToolContractError(ValueError):
    pass


class MalformedResolvedToolContractError(ValueError):
    pass


def _validate_type(value, type_or_types):
    return isinstance(value, type_or_types)


def _validate_or_raise(value, type_or_types):
    if not _validate_type(value, type_or_types):
        _d = dict(x=value, t=type(value), s=type_or_types)
        raise TypeError("Unsupported type for {x} {t}. Expected types {s}".format(**_d))
    return value


def _is_empty_list(alist):
    return len(alist) == 0


def __validate_ioputs(msg, alist):
    if _is_empty_list(alist):
        raise MalformedToolContractError(msg)
    return True


def validate_tool_contract(tc):
    """:type tc: ToolContract

    Expand this out.
    """
    __validate_ioputs("Inputs must have at least 1 input.", tc.task.input_file_types)
    __validate_ioputs("Outputs must have at least 1 output", tc.task.output_file_types)
    return tc


class _IOFileType(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, file_type_id, label, display_name, description):
        self.file_type_id = file_type_id
        self.label = label
        self.display_name = display_name
        # short description
        self.description = description

    def __repr__(self):
        _d = dict(i=self.label,
                  n=self.display_name,
                  f=self.file_type_id,
                  k=self.__class__.__name__)
        return "<{k} {f} {i} >".format(**_d)

    @abc.abstractmethod
    def to_dict(self):
        raise NotImplementedError


class InputFileType(_IOFileType):

    def to_dict(self):
        return dict(file_type_id=self.file_type_id,
                    id=self.label,
                    title=self.display_name,
                    description=self.description)


class OutputFileType(_IOFileType):

    def __init__(self, file_type_id, label, display_name, description, default_name):
        super(OutputFileType, self).__init__(file_type_id, label, display_name, description)
        # Default name of the output file. Should be specified as (base, ext)
        # but "base.ext" is also supported. This should go away
        self.default_name = default_name

    def to_dict(self):
        return dict(file_type_id=self.file_type_id,
                    id=self.label,
                    title=self.display_name,
                    description=self.description,
                    default_name=self.default_name)


class ToolContractResolvedResource(object):
    def __init__(self, resource_type_id, path):
        assert resource_type_id in ResourceTypes.ALL()
        self.type_id = resource_type_id
        self.path = path

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.type_id, p=self.path)
        return "<{k} {i} path:{p} >".format(**_d)

    @staticmethod
    def from_d(d):
        return ToolContractResolvedResource(d['resource_type'], d['path'])

    def to_dict(self):
        return dict(resource_type=self.type_id, path=self.path)


class ToolDriver(object):

    def __init__(self, driver_exe, env=None, serialization='json'):
        """

        :param driver_exe: Path to the driver
        :param env: path to env to be sourced before it's run?
        :return:
        """
        self.driver_exe = driver_exe
        self.env = {} if env is None else env
        # 'avro' or 'json'
        self.serialization = serialization

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, e=self.driver_exe)
        return "<{k} driver:{e} >".format(**_d)

    def to_dict(self):
        return dict(exe=self.driver_exe, env=self.env, serialization=self.serialization)


class ToolContractTask(object):

    TASK_TYPE_ID = TaskTypes.STANDARD

    def __init__(self, task_id, name, description, version, is_distributed, input_types, output_types, tool_options, nproc, resources):
        """
        Core metadata for a commandline task

        :param task_id: Global id to reference your tool in a pipeline
        :type task_id: str
        :param name: Display name of your
        :param description: Short description of your tool
        :param version: semantic style versioning
        :param is_distributed: If the task will be run locally or not
        :param is_distributed: bool
        :param input_types: list[FileType]
        :param output_types:
        :param tool_options:
        :param nproc:
        :param resources:
        :return:
        """
        self.task_id = task_id
        self.name = name
        self.description = description
        self.version = version
        self.is_distributed = is_distributed
        self.input_file_types = input_types
        self.output_file_types = output_types
        # This needs to be list
        # self.options = _validate_or_raise(tool_options, (list, tuple))
        self.options = tool_options
        self.nproc = nproc
        # List of ResourceTypes
        self.resources = resources

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.task_id, t=self.is_distributed, n=self.name)
        return "<{k} id:{i} {n} >".format(**_d)

    def to_dict(self):
        # this is a little hack to get around some sloppyness in the datamodel
        opts = self.options if self.options else []

        _t = dict(tool_contract_id=self.task_id,
                  input_types=[i.to_dict() for i in self.input_file_types],
                  output_types=[i.to_dict() for i in self.output_file_types],
                  task_type=self.TASK_TYPE_ID,
                  is_distributed=self.is_distributed,
                  name=self.name,
                  description=self.description,
                  schema_options=opts,
                  nproc=self.nproc,
                  resource_types=self.resources,
                  _comment="Created by v{v}".format(v=__version__))
        return _t


class ScatterToolContractTask(ToolContractTask):

    TASK_TYPE_ID = TaskTypes.SCATTERED

    def __init__(self, task_id, name, description, version, is_distributed,
                 input_types, output_types, tool_options, nproc, resources, chunk_keys, max_nchunks):
        """Scatter tasks have a special output signature of [FileTypes.CHUNK]

        The chunk keys are the expected to be written to the chunk.json file
        """
        super(ScatterToolContractTask, self).__init__(task_id, name, description, version, is_distributed,
                                                      input_types, output_types, tool_options, nproc, resources)
        self.chunk_keys = chunk_keys
        # int or $max_chunks symbol
        self.max_nchunks = max_nchunks

    def to_dict(self):
        s = super(ScatterToolContractTask, self).to_dict()
        s['chunk_keys'] = self.chunk_keys
        s['nchunks'] = self.max_nchunks
        return s


class GatherToolContractTask(ToolContractTask):
    """Gather tasks have special input type [FileTypes.CHUNK]"""
    TASK_TYPE_ID = TaskTypes.GATHERED
    # not completely sure how to handle chunk-keys.


class ToolContract(object):

    def __init__(self, task, driver):
        """

        :type task: ToolContractTask | ScatterToolContractTask | GatherToolContractTask
        :type driver: ToolDriver

        :param task:
        :param driver:
        :return:
        """
        self.task = task
        self.driver = driver

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.task.task_id, t=self.task.is_distributed)
        return "<{k} id:{i} >".format(**_d)

    def to_dict(self):
        validate_tool_contract(self)
        _t = self.task.to_dict()

        _d = dict(version=self.task.version,
                  tool_contract_id=self.task.task_id,
                  driver=self.driver.to_dict(),
                  tool_contract=_t)
        return _d


def _get_resource_by_type(rt, resources):
    xs = []
    for r in resources:
        if r.type_id == rt:
            xs.append(r)
    return xs


class ResolvedToolContractTask(object):
    # The interface is the same, but the types are "resolved" and have a
    # different
    # structure
    TASK_TYPE_ID = TaskTypes.STANDARD

    def __init__(self, task_id, is_distributed, input_files, output_files,
                 options, nproc, resources):
        self.task_id = task_id
        self.is_distributed = is_distributed
        self.input_files = input_files
        self.output_files = output_files
        self.options = options
        self.nproc = nproc
        self.resources = resources

    @property
    def tmpdir_resources(self):
        return _get_resource_by_type(ResourceTypes.TMP_DIR, self.resources)

    @property
    def tmpfile_resources(self):
        return _get_resource_by_type(ResourceTypes.TMP_FILE, self.resources)

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.task_id,
                  t=self.is_distributed)
        return "<{k} id:{i} >".format(**_d)

    def to_dict(self):
        comment = "Created by pbcommand v{v}".format(v=pbcommand.get_version())

        tc = dict(input_files=self.input_files,
                  output_files=self.output_files,
                  task_type=self.TASK_TYPE_ID,
                  is_distributed=self.is_distributed,
                  tool_contract_id=self.task_id,
                  nproc=self.nproc,
                  resources=[r.to_dict() for r in self.resources],
                  options=self.options,
                  _comment=comment)
        return tc


class ResolvedScatteredToolContractTask(ResolvedToolContractTask):
    TASK_TYPE_ID = TaskTypes.SCATTERED

    def __init__(self, task_id, is_distributed, input_files, output_files, options, nproc, resources, max_nchunks, chunk_keys):
        super(ResolvedScatteredToolContractTask, self).__init__(task_id, is_distributed, input_files, output_files, options, nproc, resources)
        self.max_nchunks = max_nchunks
        # these can be used to verified the output chunk.json
        # after the task has been run
        self.chunk_keys = chunk_keys

    def to_dict(self):
        d = super(ResolvedScatteredToolContractTask, self).to_dict()
        d['max_nchunks'] = self.max_nchunks
        d['chunk_keys'] = self.chunk_keys
        return d


class ResolvedGatherToolContractTask(ResolvedToolContractTask):
    TASK_TYPE_ID = TaskTypes.GATHERED

    def __init__(self, task_id, is_distributed, input_files, output_files, options, nproc, resources, chunk_key):
        """
        The chunk key is used in the pluck specific chunk values from
        PipelineChunks. This makes gather tasks (i.e., GffGather) generalized.
        """
        super(ResolvedGatherToolContractTask, self).__init__(task_id, is_distributed, input_files, output_files, options, nproc, resources)
        self.chunk_key = chunk_key

    def to_dict(self):
        d = super(ResolvedGatherToolContractTask, self).to_dict()
        d['chunk_key'] = self.chunk_key
        return d


class ResolvedToolContract(object):

    def __init__(self, task, driver):
        """

        :type task: ResolvedToolContractTask |
        ResolvedScatteredToolContractTask | ResolvedGatherToolContractTask
        :type driver: ToolDriver

        :param task:
        :param driver:
        :return:
        """
        self.task = task
        self.driver = driver

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.task.task_id, t=self.task.is_distributed)
        return "<{k} id:{i} >".format(**_d)

    def to_dict(self):
        return dict(resolved_tool_contract=self.task.to_dict(),
                    driver=self.driver.to_dict())
