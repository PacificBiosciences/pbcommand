"""Services Specific Data Models"""
from collections import namedtuple


# This are mirrored from the BaseSMRTServer
class LogLevels(object):
    TRACE = "TRACE"
    DEBUG = "DEBUG"
    INFO = "INFO"
    NOTICE = "NOTICE"
    WARN = "WARN"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"

    ALL = (TRACE, DEBUG, INFO, NOTICE, WARN, ERROR, CRITICAL, FATAL)

    @classmethod
    def is_valid(cls, level):
        return level in cls.ALL


SERVICE_LOGGER_RESOURCE_ID = "pbsmrtpipe"

LogResource = namedtuple("LogResource", "id name description")
LogMessage = namedtuple("LogMessage", "sourceId level message")

PbsmrtpipeLogResource = LogResource(SERVICE_LOGGER_RESOURCE_ID, "Pbsmrtpipe",
                                    "Secondary Analysis Pbsmrtpipe Job logger")


class JobExeError(ValueError):
    """Service Job failed to complete successfully"""
    pass


class SMRTServiceBaseError(Exception):
    """Fundamental Error datastructure in SMRT Server"""
    def __init__(self, http_code, error_type, message, **kwargs):
        self.http_code = http_code
        self.error_type = error_type
        self.msg = message
        message = "Http code={h} msg={m} type={t}".format(h=http_code, m=message, t=error_type)
        super(Exception, self).__init__(message)

    @staticmethod
    def from_d(d):
        return SMRTServiceBaseError(d['httpCode'], d['errorType'], d['message'])


# "Job" is the raw output from the jobs/1234
JobResult = namedtuple("JobResult", "job run_time errors")


class ServiceEntryPoint(object):
    """Entry Points to initialize Pipelines"""
    def __init__(self, entry_id, dataset_type, path_or_uri):
        self.entry_id = entry_id
        self.dataset_type = dataset_type
        # int (only supported), UUID or path to XML dataset will be added
        self._resource = path_or_uri

    @property
    def resource(self):
        return self._resource

    def __repr__(self):
        return "<{k} {e} {d} {r} >".format(k=self.__class__.__name__, e=self.entry_id, r=self._resource, d=self.dataset_type)


class JobStates(object):
    RUNNING = "RUNNING"
    CREATED = "CREATED"
    FAILED = "FAILED"
    SUCCESSFUL = "SUCCESSFUL"

    ALL = (RUNNING, CREATED, FAILED)

    # End points
    ALL_COMPLETED = (FAILED, SUCCESSFUL)


class JobTypes(object):
    IMPORT_DS = "import-dataset"
    IMPORT_DSTORE = "import-datastore"
    MERGE_DS = "merge-datasets"
    PB_PIPE = "pbsmrtpipe"
    MOCK_PB_PIPE = "mock-pbsmrtpipe"
    CONVERT_FASTA = 'convert-fasta-reference'


class ServiceResourceTypes(object):
    REPORTS = "reports"
    DATASTORE = "datastore"
