"""Services Specific Data Models"""
from collections import namedtuple
import uuid

import iso8601

from requests.exceptions import RequestException


def to_ascii(s):
    return s.encode('ascii', 'ignore')


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


class ServiceJob(namedtuple("ServiceJob", 'id uuid name state path job_type created_at')):

    @staticmethod
    def from_d(d):
        def sx(x):
            return d[x]

        def se(x):
            return sx(x).encode('ascii', 'ignore')

        def to_t(x):
            return iso8601.parse_date(se(x))

        return ServiceJob(sx('id'), sx('uuid'), se('name'), se('state'),
                          se('path'), se('jobTypeId'), to_t('createdAt'))

    def was_successful(self):
        return self.state == JobStates.SUCCESSFUL


class JobExeError(ValueError):
    """Service Job failed to complete successfully"""
    pass


class SmrtServerConnectionError(RequestException):
    """This is blunt to catch all status related errors"""
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


def _to_resource_id(x):
    if isinstance(x, int):
        return x
    try:
        _ = uuid.UUID(x)
        return x
    except ValueError as e:
        raise ValueError("Resource id '{x}' must be given as int or uuid".format(x=x))


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

    @staticmethod
    def from_d(d):
        i = _to_resource_id(d['datasetId'])
        return ServiceEntryPoint(to_ascii(d['entryId']), to_ascii(d['fileTypeId']), i)

    def to_d(self):
        return dict(entryId=self.entry_id,
                    fileTypeId=self.dataset_type,
                    datasetId=self.resource)


class JobEntryPoint(namedtuple("JobEntryPoint", "job_id dataset_uuid dataset_metatype")):
    """ Returned from the Services /job/1234/entry-points """
    @staticmethod
    def from_d(d):
        return JobEntryPoint(d['jobId'], d['datasetUUID'], d['datasetType'])


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

    @classmethod
    def ALL(cls):
        return (cls.IMPORT_DS, cls.IMPORT_DSTORE, cls.MERGE_DS,
                cls.PB_PIPE, cls.MOCK_PB_PIPE, cls.CONVERT_FASTA)


class ServiceResourceTypes(object):
    REPORTS = "reports"
    DATASTORE = "datastore"
    ENTRY_POINTS = "entry-points"
