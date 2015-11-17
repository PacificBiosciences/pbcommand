"""Utils for Updating state/progress and results to WebServices


"""
import json
import logging
import pprint
from collections import namedtuple
import requests
import time

from pbcommand.models import FileTypes

log = logging.getLogger(__name__)


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


HEADERS = {'Content-type': 'application/json'}
SERVICE_LOGGER_RESOURCE_ID = "pbsmrtpipe"

LogResource = namedtuple("LogResource", "id name description")
LogMessage = namedtuple("LogMessage", "sourceId level message")

PbsmrtpipeLogResource = LogResource(SERVICE_LOGGER_RESOURCE_ID, "Pbsmrtpipe",
                                    "Secondary Analysis Pbsmrtpipe Job logger")


class JobExeError(ValueError):
    """Service Job Failure"""
    pass

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
    PB_PIPE = "pbsmrtpipe"
    MOCK_PB_PIPE = "mock-pbsmrtpipe"


class ServiceResourceTypes(object):
    REPORTS = "reports"
    DATASTORE = "datastore"


def _post_requests(headers):
    def wrapper(url, d_):
        data = json.dumps(d_)
        return requests.post(url, data=data, headers=headers)

    return wrapper


def _get_requests(headers):
    def wrapper(url):
        return requests.get(url, headers=headers)

    return wrapper


rqpost = _post_requests(HEADERS)
rqget = _get_requests(HEADERS)


def _process_rget(func):
    # apply the tranform func to the output of GET request if it was successful
    def wrapper(total_url):
        r = rqget(total_url)
        if r.status_code != 200:
            log.error("Failed ({s}) GET to {x}".format(x=total_url, s=r.status_code))
        r.raise_for_status()
        j = r.json()
        return func(j)

    return wrapper


def _process_rpost(func):
    # apply the transform func to the output of POST request if it was successful
    def wrapper(total_url, payload_d):
        r = rqpost(total_url, payload_d)
        if r.status_code != 200:
            log.error("Failed ({s} to call {u}".format(u=total_url, s=r.status_code))
            log.error("payload")
            log.error("\n" + pprint.pformat(payload_d))
        r.raise_for_status()
        j = r.json()
        return func(j)

    return wrapper


def _to_url(base, ext):
    return "".join([base, ext])


def _null_func(x):
    # Pass thorough func
    return x


def _import_dataset_by_type(dataset_type_id):
    def wrapper(total_url, path):
        _d = dict(datasetType=dataset_type_id, path=path)
        f = _process_rpost(_null_func)
        return f(total_url, _d)

    return wrapper


def _block_for_job_to_complete(sal, job_id, time_out=600):
    def _to_ascii(v):
        return v.encode('ascii', 'ignore')

    job = sal.get_job_by_id(job_id)
    state = _to_ascii(job['state'])
    name = _to_ascii(job['name'])

    job_result = JobResult(job, 0, "")
    started_at = time.time()
    # in seconds
    sleep_time = 2
    while True:
        run_time = time.time() - started_at
        if state in JobStates.ALL_COMPLETED:
            break
        log.debug("Running pipeline {n} state: {s} runtime:{r:.2f} sec".format(n=name, s=state, r=run_time))
        time.sleep(sleep_time)
        job = sal.get_job_by_id(job_id)
        state = _to_ascii(job['state'])
        job_result = JobResult(job, run_time, "")
        if time_out is not None:
            if run_time > time_out:
                raise JobExeError("Exceeded runtime {r} of {t}".format(r=run_time, t=time_out))

    return job_result

# Make this consistent somehow. Maybe defined 'shortname' in the core model?
# Martin is doing this for the XML file names
DATASET_METATYPES_TO_ENDPOINTS = {
     FileTypes.DS_SUBREADS_H5: "hdfsubreads",
     FileTypes.DS_SUBREADS: "subreads",
     FileTypes.DS_ALIGN: "alignments",
     FileTypes.DS_REF: "references",
     FileTypes.DS_BARCODE: "barcodes",
     FileTypes.DS_CCS: "ccsreads",
     FileTypes.DS_CONTIG: "contigs",
     FileTypes.DS_ALIGN_CCS: "css-alignments"}


def _get_endpoint_or_raise(ds_type):
    if ds_type in DATASET_METATYPES_TO_ENDPOINTS:
        return DATASET_METATYPES_TO_ENDPOINTS[ds_type]
    raise KeyError("Unsupported datasettype {t}. Supported values {v}".format(t=ds_type, v=DATASET_METATYPES_TO_ENDPOINTS.keys()))


def _job_id_or_error(job_or_error, custom_err_msg=None):
    """
    Extract job id from job creation service (by type)
    or Raise exception from an EngineJob response

    :raises: JobExeError
    """
    if 'id' in job_or_error:
        job_id = job_or_error['id']
        log.info("created job {i}".format(i=job_id))
        return job_id
    else:
        emsg = job_or_error.get('message', "Unknown")
        if custom_err_msg is not None:
            emsg += " {f}".format(f=custom_err_msg)
        raise JobExeError("Failed to create job. {e}. Raw Response {x}".format(e=emsg, x=job_or_error))


class ServiceAccessLayer(object):
    """General Access Layer for interfacing with the job types on Secondary SMRT Server"""

    # in sec when blocking to run a job
    JOB_DEFAULT_TIMEOUT = 60 * 30

    def __init__(self, base_url, port, debug=False):
        self.base_url = base_url
        self.port = port
        # This will display verbose details with respect to the failed request
        self.debug = debug

    @property
    def uri(self):
        return "{b}:{u}".format(b=self.base_url, u=self.port)

    def _to_url(self, rest):
        return _to_url(self.uri, rest)

    def __repr__(self):
        return "<{k} {u} >".format(k=self.__class__.__name__, u=self.uri)

    def get_status(self):
        """Get status of the server"""
        return _process_rget(_null_func)(_to_url(self.uri, "/status"))

    def get_job_by_id(self, job_id):
        """Get a Job by int id"""
        return _process_rget(_null_func)(_to_url(self.uri, "/secondary-analysis/job-manager/jobs/{i}".format(i=job_id)))

    def get_job_by_type_and_id(self, job_type, job_id):
        return _process_rget(_null_func)(_to_url(self.uri, "/secondary-analysis/job-manager/jobs/{t}/{i}".format(i=job_id, t=job_type)))

    def _get_job_resource_type(self, job_type, job_id, resource_type_id):
        # grab the datastore or the reports
        _d = dict(t=job_type, i=job_id,r=resource_type_id)
        return _process_rget(_null_func)(_to_url(self.uri,"/secondary-analysis/job-manager/jobs/{t}/{i}/{r}".format(**_d)))

    def _import_dataset(self, dataset_type, path):
        # This returns a job resource
        return _import_dataset_by_type(dataset_type)(self._to_url("/secondary-analysis/job-manager/jobs/import-dataset"), path)

    def run_import_dataset_by_type(self, dataset_type, path_to_xml):
        job_or_error = self._import_dataset(dataset_type, path_to_xml)
        custom_err_msg = "Import {d} {p}".format(p=path_to_xml, d=dataset_type)
        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self, job_id)

    def _run_import_and_block(self, func, path, time_out=None):
        # func while be self.import_dataset_X
        job_or_error = func(path)
        custom_err_msg = "Import {p}".format(p=path)
        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self, job_id, time_out=time_out)

    def import_dataset_subread(self, path):
        return self._import_dataset(FileTypes.DS_SUBREADS, path)

    def run_import_dataset_subread(self, path, time_out=10):
        return self._run_import_and_block(self.import_dataset_subread, path, time_out=time_out)

    def import_dataset_hdfsubread(self, path):
        return self._import_dataset(FileTypes.DS_SUBREADS_H5, path)

    def run_import_dataset_hdfsubread(self, path, time_out=10):
        return self._run_import_and_block(self.import_dataset_hdfsubread, path, time_out=time_out)

    def import_dataset_reference(self, path):
        return self._import_dataset(FileTypes.DS_REF, path)

    def run_import_dataset_reference(self, path, time_out=10):
        return self._run_import_and_block(self.import_dataset_reference, path, time_out=time_out)

    def get_dataset_by_id(self, dataset_type, int_or_uuid):
        """Get a Dataset using the DataSetMetaType and (int|uuid) of the dataset"""
        ds_endpoint = _get_endpoint_or_raise(dataset_type)
        return _process_rget(_null_func)(_to_url(self.uri, "/secondary-analysis/datasets/{t}/{i}".format(t=ds_endpoint, i=int_or_uuid)))

    def _get_datasets_by_type(self, dstype):
        return _process_rget(_null_func)(_to_url(self.uri, "/secondary-analysis/datasets/{i}".format(i=dstype)))

    def get_subreadset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_SUBREADS, int_or_uuid)

    def get_subreadsets(self):
        return self._get_datasets_by_type("subreads")

    def get_hdfsubreadset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_SUBREADS_H5, int_or_uuid)

    def get_hdfsubreadsets(self):
        return self._get_datasets_by_type("hdfsubreads")

    def get_referenceset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_REF, int_or_uuid)

    def get_referencesets(self):
        return self._get_datasets_by_type("references")

    def get_alignmentset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_ALIGN, int_or_uuid)

    def get_alignmentsets(self):
        return self._get_datasets_by_type("alignments")

    def import_fasta(self, fasta_path, name, organism, ploidy):
        """Convert fasta file to a ReferenceSet and Import. Returns a Job """
        d = dict(path=fasta_path,
                 name=name,
                 organism=organism,
                 ploidy=ploidy)

        return _process_rpost(_null_func)(self._to_url("/secondary-analysis/job-manager/jobs/convert-fasta-reference"), d)

    def run_import_fasta(self, fasta_path, name, organism, ploidy, time_out=JOB_DEFAULT_TIMEOUT):
        """Import a Reference into a Block"""""
        job_or_error = self.import_fasta(fasta_path, name, organism, ploidy)
        _d = dict(f=fasta_path, n=name, o=organism, p=ploidy)
        custom_err_msg = "Fasta-convert path:{f} name:{n} organism:{o} ploidy:{p}".format(**_d)
        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self, job_id, time_out=time_out)

    def create_logger_resource(self, idx, name, description):
        _d = dict(id=idx, name=name, description=description)
        return _process_rpost(_null_func)(_to_url(self.uri, "/loggers"), _d)

    def log_progress_update(self, job_type_id, job_id, message, level, source_id):
        """This is the generic job logging mechanism"""
        _d = dict(message=message, level=level, sourceId=source_id)
        return _process_rpost(_null_func)(_to_url(self.uri, "/secondary-analysis/job-manager/jobs/{t}/{i}/log".format(t=job_type_id, i=job_id)), _d)

    def get_pipeline_template_by_id(self, pipeline_template_id):
        return _process_rget(_null_func)(_to_url(self.uri, "/secondary-analysis/resolved-pipeline-templates/{i}".format(i=pipeline_template_id)))

    def create_by_pipeline_template_id(self, name, pipeline_template_id, epoints):
        """Creates and runs a pbsmrtpipe pipeline by pipeline template id"""
        # sanity checking to see if pipeline is valid
        _ = self.get_pipeline_template_by_id(pipeline_template_id)

        seps = [dict(entryId=e.entry_id, fileTypeId=e.dataset_type, datasetId=e.resource) for e in epoints]

        def _to_o(opt_id, opt_value, option_type_id):
            return dict(optionId=opt_id, value=opt_value, optionTypeId=option_type_id)

        # FIXME. Need to define this in the scenario IO layer.
        # task_options = [_to_o("option_01", "value_01")]
        # workflow_options = [_to_o("woption_01", "value_01")]
        task_options = []
        workflow_options = []
        d = dict(name=name, pipelineId=pipeline_template_id, entryPoints=seps, taskOptions=task_options, workflowOptions=workflow_options)
        return _process_rpost(_null_func)(_to_url(self.uri, "/secondary-analysis/job-manager/jobs/{p}".format(p=JobTypes.PB_PIPE)), d)

    def run_by_pipeline_template_id(self, name, pipeline_template_id, epoints, time_out=JOB_DEFAULT_TIMEOUT):
        """Blocks and runs a job with a timeout"""

        job_or_error = self.create_by_pipeline_template_id(name, pipeline_template_id, epoints)

        _d = dict(name=name, p=pipeline_template_id, eps=epoints)
        custom_err_msg = "Job {n} args: {a}".format(n=name, a=_d)

        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self, job_id, time_out=time_out)


def log_pbsmrtpipe_progress(total_url, message, level, source_id, ignore_errors=True):
    """Log the status of a pbsmrtpipe to SMRT Server"""

    # Need to clarify the model here. Trying to pass the most minimal
    # data necessary to pbsmrtpipe.
    _d = dict(message=message, level=level, sourceId=source_id)
    func = _process_rpost(_null_func)
    if ignore_errors:
        try:
            return func(total_url, _d)
        except Exception as e:
            log.warn("Failed Request to {u} data: {d}. {e}".format(u=total_url, d=_d, e=e))
    else:
        return func(total_url, _d)


def add_datastore_file(total_url, datastore_file, ignore_errors=True):
    """Add datastore to SMRT Server

    :type datastore_file: DataStoreFile
    """
    _d = datastore_file.to_dict()
    func = _process_rpost(_null_func)
    if ignore_errors:
        try:
            return func(total_url, _d)
        except Exception as e:
            log.warn("Failed Request to {u} data: {d}. {e}".format(u=total_url, d=_d, e=e))
    else:
        return func(total_url, _d)

