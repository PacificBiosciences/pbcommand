"""Common models for Tool Contract and Resolved Tool Contract


Author: Michael Kocher
"""
import abc
import datetime

__version__ = '0.2.0'


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

    def __init__(self, file_type, label, display_name, description, default_name):
        super(OutputFileType, self).__init__(file_type, label, display_name, description)
        # Default name of the output file. Should be specified as (base, ext)
        # but "base.ext" is also supported. This should go away
        self.default_name = default_name

    def to_dict(self):
        return dict(file_type_id=self.file_type_id,
                    id=self.label,
                    title=self.display_name,
                    description=self.description,
                    default_name=self.default_name)


class ToolDriver(object):

    def __init__(self, driver_exe, env=None):
        """

        :param driver_exe: Path to the driver
        :param env: path to env to be sourced before it's run?
        :return:
        """
        self.driver_exe = driver_exe
        self.env = {} if env is None else env

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, e=self.driver_exe)
        return "<{k} driver:{e} >".format(**_d)

    def to_dict(self):
        return dict(exe=self.driver_exe, env=self.env)


class ToolContractTask(object):

    def __init__(self, task_id, name, description, version, task_type, input_types, output_types, tool_options, nproc, resources):
        """
        Core metadata for a commandline task

        :param task_id: Global id to reference your tool in a pipeline
        :type task_id: str
        :param name: Display name of your
        :param description: Short description of your tool
        :param version: semantic style versioning
        :param task_type: If the task will be run locally or not
        :parama task_type: TaskTypes.LOCAL | TaskTypes.DISTRIBUTED
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
        self.task_type = task_type
        self.input_file_types = input_types
        self.output_file_types = output_types
        self.options = tool_options
        self.nproc = nproc
        self.resources = resources

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.task_id, t=self.task_type, n=self.name)
        return "<{k} id:{i} {n} >".format(**_d)

    def to_dict(self):
        created_at = datetime.datetime.now()
        _t = dict(input_types=[i.to_dict() for i in self.input_file_types],
                  output_types=[i.to_dict() for i in self.output_file_types],
                  task_type=self.task_type,
                  schema_options=self.options,
                  nproc=self.nproc,
                  resource_types=self.resources,
                  _comment="Created by v{v} at {d}".format(v=__version__, d=created_at.isoformat()))
        return _t


class ResolvedToolContractTask(object):
    # The interface is the same, but the types are "resolved" and have a different
    # structure

    def __init__(self, task_id, task_type, input_files, output_files, options, nproc, resources):
        self.task_id = task_id
        self.task_type = task_type
        self.input_files = input_files
        self.output_files = output_files
        self.options = options
        self.nproc = nproc
        self.resources = resources

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.task_id, t=self.task_type)
        return "<{k} id:{i} >".format(**_d)


class ToolContract(object):

    def __init__(self, task, driver):
        """

        :type task: ToolContractTask
        :type driver: ToolDriver

        :param task:
        :param driver:
        :return:
        """
        self.task = task
        self.driver = driver

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.task.task_id, t=self.task.task_type)
        return "<{k} id:{i} >".format(**_d)

    def to_dict(self):
        _t = self.task.to_dict()
        _d = dict(version=self.task.version,
                  tool_contract_id=self.task.task_id,
                  driver=self.driver.to_dict(),
                  tool_contract=_t)
        return _d


class ResolvedToolContract(object):

    def __init__(self, task, driver):
        """

        :type task: ResolvedToolContractTask
        :type driver: ToolDriver

        :param task:
        :param driver:
        :return:
        """
        self.task = task
        self.driver = driver

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.task.task_id, t=self.task.task_type)
        return "<{k} id:{i} >".format(**_d)
