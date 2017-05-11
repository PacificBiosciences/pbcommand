"""Services Specific Data Models"""
from collections import namedtuple
import json
import uuid

import iso8601

from requests.exceptions import RequestException

from pbcommand.utils import to_ascii

__all__ = ['ServiceJob', 'ServiceEntryPoint', 'JobEntryPoint', 'JobStates', 'JobTypes']


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


class ServiceJob(object):
    def __init__(self, ix, job_uuid, name, state, path, job_type, created_at,
                 settings, is_active=True, smrtlink_version=None,
                 created_by=None, updated_at=None, error_message=None):
        """

        :param ix: Job Integer Id
        :param job_uuid: Globally unique Job UUID
        :param name:  Display name of job
        :param state:  Job State
        :param path: Absolute Path to the job directory
        :param job_type:  Job Type
        :param created_at: when the job was created at
        :param settings: dict of job specific settings
        :param is_active:  If the Job is active (only active jobs are displayed in the SL UI)
        :param smrtlink_version: SMRT Link Version (if known)
        :param created_by: User that created the job
        :param updated_at: when the last update of the job occurred
        :param error_message: Error message if the job has failed

        :type ix: int
        :type job_uuid: str
        :type name: str
        :type state: str
        :type job_type: str
        :type created_at: DateTime
        :type updated_at: DateTime | None
        :type settings: dict
        :type is_active: bool
        :type smrtlink_version: str | None
        :type created_by: str | None
        :type error_message: str | None
        """
        self.id = int(ix)
        # validation
        _ = uuid.UUID(job_uuid)
        self.uuid = job_uuid
        self.name = name
        self.state = state
        self.path = path
        self.job_type = job_type
        self.created_at = created_at
        self.settings = settings
        self.is_active = is_active
        self.smrtlink_version = smrtlink_version
        self.created_by = created_by
        # Is this Option[T] or T?
        self.updated_at = updated_at
        self.error_message = error_message

        if self.updated_at is not None:
            dt = self.updated_at - self.created_at
            self.run_time_sec = dt.total_seconds()
        else:
            self.run_time_sec = None

    def __repr__(self):
        # truncate the name to avoid having a useless repr
        max_length = 15
        if len(self.name) >= max_length:
            name = self.name[:max_length] + "..."
        else:
            name = self.name

        created_by = "Unknown" if self.created_by is None else self.created_by

        ix = str(self.id).rjust(5)
        state = self.state.rjust(11)
        # simpler format
        created_at = self.created_at.strftime("%m-%d-%Y %I:%M.%S")

        # this should really use humanize. But this would take forever
        # to get into the nightly build
        def _format_dt(n_seconds):
            if n_seconds >= 60:
                # for most cases, you don't really don't
                # care about the seconds
                return "{m} min ".format(m=int(n_seconds / 60))
            else:
                return "{s:.2f} sec".format(s=n_seconds)

        run_time = "NA" if self.run_time_sec is None else _format_dt(self.run_time_sec)

        _d = dict(k=self.__class__.__name__,
                  i=ix,
                  n=name,
                  c=created_at,
                  u=self.uuid,
                  s=state, b=created_by,
                  r=run_time)

        return "<{k} i:{i} state:{s} created:{c} by:{b} name:{n} runtime: {r} >".format(**_d)

    @staticmethod
    def from_d(d):
        """Convert from Service JSON response to `ServiceJob` instance"""

        def sx(x):
            return d[x]

        def s_or(x, default=None):
            return d.get(x, default)

        # Convert to string key value to ascii
        def se(x):
            return to_ascii(sx(x))

        def se_or(x, default=None):
            v = s_or(x, default=default)
            return v if v is None else to_ascii(v)

        def to_t(x):
            return iso8601.parse_date(se(x))

        def to_d(x):
            # the "jsonSettings" are a string for some stupid reason
            return json.loads(sx(x))

        def to_opt_datetime(k):
            x = s_or(k)
            return iso8601.parse_date(x) if x is not None else None

        ix = int(sx('id'))
        job_uuid = sx('uuid')
        name = se('name')
        state = se('state')
        path = se('path')
        job_type = se('jobTypeId')
        created_at = to_t('createdAt')
        updated_at = to_opt_datetime('updatedAt')

        smrtlink_version = se_or("smrtlinkVersion")
        error_message = se_or("errorMessage")
        created_by = se_or("createdBy")
        is_active = d.get('isActive', True)
        settings = to_d('jsonSettings')

        return ServiceJob(ix, job_uuid, name, state, path, job_type,
                          created_at,
                          settings, is_active=is_active,
                          smrtlink_version=smrtlink_version,
                          created_by=created_by,
                          updated_at=updated_at,
                          error_message=error_message)

    def was_successful(self):
        """ :rtype: bool """
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
        super(SMRTServiceBaseError, self).__init__(message)

    @staticmethod
    def from_d(d):
        """Convert from SMRT Link Service Error JSON response to `SMRTServiceBaseError` instance"""
        return SMRTServiceBaseError(d['httpCode'], d['errorType'], d['message'])


# "Job" is the raw output from the jobs/1234
JobResult = namedtuple("JobResult", "job run_time errors")


class JobTask(namedtuple("JobTask", "task_uuid job_id task_id task_type name state created_at updated_at error_message")):

    @staticmethod
    def from_d(d):
        return JobTask(d['uuid'], d['jobId'], d['taskId'], d['taskTypeId'],
                       d['name'], d['state'], d['createdAt'],
                       d['updatedAt'], d.get('errorMessage'))


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
        """Backwards compatible with path_or_uri"""
        return self._resource

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, e=self.entry_id,
                  r=self._resource, d=self.dataset_type)
        return "<{k} {e} {d} {r} >".format(**_d)

    @staticmethod
    def from_d(d):
        """Convert from Service JSON response to `ServiceEntryPoint` instance"""
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
        """Convert from Service JSON response to `JobEntryPoint` instance"""
        return JobEntryPoint(d['jobId'], d['datasetUUID'], d['datasetType'])


class JobStates(object):
    """Allowed SMRT Link Service Job states"""
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCESSFUL = "SUCCESSFUL"

    ALL = (RUNNING, CREATED, FAILED, SUCCESSFUL, SUBMITTED)

    # End points
    ALL_COMPLETED = (FAILED, SUCCESSFUL)


class JobTypes(object):
    """SMRT Link Analysis JOb Types"""
    IMPORT_DS = "import-dataset"
    IMPORT_DSTORE = "import-datastore"
    MERGE_DS = "merge-datasets"
    PB_PIPE = "pbsmrtpipe"
    MOCK_PB_PIPE = "mock-pbsmrtpipe"
    CONVERT_FASTA = 'convert-fasta-reference'

    @classmethod
    def ALL(cls):
        """ALL allowed SL Analysis Job Types"""
        return (cls.IMPORT_DS, cls.IMPORT_DSTORE, cls.MERGE_DS,
                cls.PB_PIPE, cls.MOCK_PB_PIPE, cls.CONVERT_FASTA)


class ServiceResourceTypes(object):
    REPORTS = "reports"
    DATASTORE = "datastore"
    ENTRY_POINTS = "entry-points"
