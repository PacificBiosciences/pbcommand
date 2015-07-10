"""Common models for Tool Contract and Resolved Tool Contract


Author: Michael Kocher
"""


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
