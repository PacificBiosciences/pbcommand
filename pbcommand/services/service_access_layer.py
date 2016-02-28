"""Utils for Updating state/progress and results to WebServices


"""
import json
import logging
import pprint
import time

import requests
from requests import RequestException

from pbcommand.models import (FileTypes,
                              DataSetFileType,
                              DataStore,
                              DataStoreFile)
from pbcommand.utils import get_dataset_metadata

from .models import (SMRTServiceBaseError,
                     JobResult, JobStates, JobExeError, JobTypes,
                     LogLevels, ServiceEntryPoint,
                     ServiceResourceTypes, ServiceJob, JobEntryPoint)

from .utils import to_ascii, to_sal_summary

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())  # to prevent the annoying 'No handlers .. ' msg


class Constants(object):
    HEADERS = {'Content-type': 'application/json'}


def _post_requests(headers):
    def wrapper(url, d_):
        data = json.dumps(d_)
        return requests.post(url, data=data, headers=headers)

    return wrapper


def _get_requests(headers):
    def wrapper(url):
        return requests.get(url, headers=headers)

    return wrapper

# These are exposed publicly as a utility, but shouldn't be used in any API
# call. The _process_* are the entry points for API calls to make sure an
# errors are handled correctly.
rqpost = _post_requests(Constants.HEADERS)
rqget = _get_requests(Constants.HEADERS)


def _parse_base_service_error(response):
    """:type response: requests.Response

    Don't trust the services. Try to parse the response to SMRT Server Error
    datastructure (even if a 200 is returned)
    """
    if response.ok:
        try:
            d = response.json()
            emsg = SMRTServiceBaseError.from_d(d)
            raise emsg
        except (KeyError, TypeError):
            # couldn't parse response -> error,
            # so everything is fine
            return response
    else:
        return response


def _process_rget(total_url):
    """Process get request and return JSON response. Raise if not successful"""
    r = rqget(total_url)
    _parse_base_service_error(r)
    if not r.ok:
        log.error("Failed ({s}) GET to {x}".format(x=total_url, s=r.status_code))
    r.raise_for_status()
    j = r.json()
    return j


def _process_rget_with_transform(func):
    """Post process the JSON result (if successful) with F(json_d) -> T"""
    def wrapper(total_url):
        j = _process_rget(total_url)
        return func(j)
    return wrapper


def _process_rget_with_jobs_transform(total_url):
    # defining an internal method, because this used in several places
    jobs_d = _process_rget(total_url)
    return [ServiceJob.from_d(job_d) for job_d in jobs_d]


def _process_rget_or_none(func):
    """
    apply the transform func to the output of GET request if it was successful, else returns None

    This is intended to be used for looking up Results by Id where the a 404
    is found.
    """
    def wrapper(total_url):
        try:
            return _process_rget_with_transform(func)(total_url)
        except (RequestException, SMRTServiceBaseError):
            # FIXME
            # this should be a tighter exception case
            # only look for 404
            return None

    return wrapper


def _process_rget_with_job_transform_or_none(total_url):
    return _process_rget_or_none(ServiceJob.from_d)(total_url)


def _process_rpost(total_url, payload_d):
    r = rqpost(total_url, payload_d)
    _parse_base_service_error(r)
    # FIXME This should be strict to only return a 201
    if r.status_code not in (200, 201):
        log.error("Failed ({s} to call {u}".format(u=total_url, s=r.status_code))
        log.error("payload")
        log.error("\n" + pprint.pformat(payload_d))
    r.raise_for_status()
    j = r.json()
    return j


def _process_rpost_with_transform(func):
    def wrapper(total_url, payload_d):
        j = _process_rpost(total_url, payload_d)
        return func(j)
    return wrapper


def _to_url(base, ext):
    return "".join([base, ext])


def _null_func(x):
    # Pass thorough func
    return x


def _import_dataset_by_type(dataset_type_or_id):

    if isinstance(dataset_type_or_id, DataSetFileType):
        ds_type_id = dataset_type_or_id.file_type_id
    else:
        ds_type_id = dataset_type_or_id

    def wrapper(total_url, path):
        _d = dict(datasetType=ds_type_id, path=path)
        return _process_rpost_with_transform(ServiceJob.from_d)(total_url, _d)

    return wrapper


def _get_job_by_id_or_raise(sal, job_id, error_klass, error_messge_extras=None):
    job = sal.get_job_by_id(job_id)

    if job is None:
        details = "" if error_messge_extras is None else error_messge_extras
        base_msg = "Failed to find job {i}".format(i=job_id)
        emsg = " ".join([base_msg, details])
        raise error_klass(emsg)

    return job


def _block_for_job_to_complete(sal, job_id, time_out=600, sleep_time=2):
    """
    Waits for job to complete

    :param sal: ServiceAccessLayer
    :param job_id: Job Id
    :param time_out: Total runtime before aborting
    :param sleep_time: polling interval (in sec)

    :rtype: JobResult
    :raises: KeyError if job is not initially found, or JobExeError
    if the job fails during the polling process or times out
    """

    time.sleep(sleep_time)
    job = _get_job_by_id_or_raise(sal, job_id, KeyError)

    log.debug("time_out = {t}".format(t=time_out))

    error_msg = ""
    job_result = JobResult(job, 0, error_msg)
    started_at = time.time()

    # number of polling steps
    i = 0
    while True:
        run_time = time.time() - started_at

        if job.state in JobStates.ALL_COMPLETED:
            break

        i += 1
        time.sleep(sleep_time)

        msg = "Running pipeline {n} state: {s} runtime:{r:.2f} sec {i} iteration".format(n=job.name, s=job.state, r=run_time, i=i)
        log.debug(msg)
        # making the exceptions different to distinguish between an initial
        # error and a "polling" error. Adding some msg details
        job = _get_job_by_id_or_raise(sal, job_id, JobExeError, error_messge_extras=msg)

        # FIXME, there's currently not a good way to get errors for jobs
        job_result = JobResult(job, run_time, "")
        if time_out is not None:
            if run_time > time_out:
                raise JobExeError("Exceeded runtime {r} of {t}. {m}".format(r=run_time, t=time_out, m=msg))

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
    if isinstance(job_or_error, ServiceJob):
        return job_or_error.id
    else:
        emsg = job_or_error.get('message', "Unknown")
        if custom_err_msg is not None:
            emsg += " {f}".format(f=custom_err_msg)
        raise JobExeError("Failed to create job. {e}. Raw Response {x}".format(e=emsg, x=job_or_error))


def _to_host(h):
    prefix = "http://"
    return h if h.startswith(prefix) else prefix + h


def _to_ds_file(d):
    # is_chunk this isn't exposed at the service level
    return DataStoreFile(d['uuid'], d['sourceId'], d['fileTypeId'], d['path'], is_chunked=False)


def _to_datastore(dx):
    # Friction to get around service endpoint not returning a list of files
    ds_files = [_to_ds_file(d) for d in dx]
    return DataStore(ds_files)


def _to_entry_points(d):
    return [JobEntryPoint.from_d(i) for i in d]


class ServiceAccessLayer(object):
    """General Access Layer for interfacing with the job types on Secondary SMRT Server"""

    ROOT_JM = "/secondary-analysis/job-manager"
    ROOT_JOBS = ROOT_JM + "/jobs"
    ROOT_DS = "/secondary-analysis/datasets"
    ROOT_PT = '/secondary-analysis/resolved-pipeline-templates'

    # in sec when blocking to run a job
    JOB_DEFAULT_TIMEOUT = 60 * 30

    def __init__(self, base_url, port, debug=False, sleep_time=2):
        self.base_url = _to_host(base_url)
        self.port = port
        # This will display verbose details with respect to the failed request
        self.debug = debug
        self._sleep_time = sleep_time

    @property
    def uri(self):
        return "{b}:{u}".format(b=self.base_url, u=self.port)

    def _to_url(self, rest):
        return _to_url(self.uri, rest)

    def __repr__(self):
        return "<{k} {u} >".format(k=self.__class__.__name__, u=self.uri)

    def to_summary(self):
        return to_sal_summary(self)

    def get_status(self):
        """Get status of the server"""
        return _process_rget(_to_url(self.uri, "/status"))

    def get_job_by_type_and_id(self, job_type, job_id):
        return _process_rget_with_job_transform_or_none(_to_url(self.uri, "{p}/{t}/{i}".format(i=job_id, t=job_type, p=ServiceAccessLayer.ROOT_JOBS)))

    def get_job_by_id(self, job_id):
        """Get a Job by int id"""
        # FIXME. Make this an internal method It's ambiguous which job type type you're asking for
        return _process_rget_with_job_transform_or_none(_to_url(self.uri, "{r}/{i}".format(i=job_id, r=ServiceAccessLayer.ROOT_JOBS)))

    def _get_job_resource_type(self, job_type, job_id, resource_type_id):
        # grab the datastore or the reports
        _d = dict(t=job_type, i=job_id, r=resource_type_id, p=ServiceAccessLayer.ROOT_JOBS)
        return _process_rget_with_job_transform_or_none(_to_url(self.uri, "{p}/{t}/{i}/{r}".format(**_d)))

    def _get_job_resource_type_with_transform(self, job_type, job_id, resource_type_id, transform_func):
        _d = dict(t=job_type, i=job_id, r=resource_type_id, p=ServiceAccessLayer.ROOT_JOBS)
        return _process_rget_or_none(transform_func)(_to_url(self.uri, "{p}/{t}/{i}/{r}".format(**_d)))

    def _get_jobs_by_job_type(self, job_type):
        return _process_rget_with_jobs_transform(_to_url(self.uri, "{p}/{t}".format(t=job_type, p=ServiceAccessLayer.ROOT_JOBS)))

    def get_analysis_jobs(self):
        return self._get_jobs_by_job_type(JobTypes.PB_PIPE)

    def get_import_dataset_jobs(self):
        return self._get_jobs_by_job_type(JobTypes.IMPORT_DS)

    def get_merge_dataset_jobs(self):
        return self._get_jobs_by_job_type(JobTypes.MERGE_DS)

    def get_fasta_convert_jobs(self):
        self._get_jobs_by_job_type(JobTypes.CONVERT_FASTA)

    def get_analysis_job_by_id(self, job_id):
        """Get an Analysis job by id or UUID or return None

        :rtype: ServiceJob
        """
        return self.get_job_by_type_and_id(JobTypes.PB_PIPE, job_id)

    def get_analysis_job_datastore(self, job_id):
        """Get DataStore output from (pbsmrtpipe) analysis job"""
        # this doesn't work the list is sli
        return self._get_job_resource_type_with_transform(JobTypes.PB_PIPE, job_id, ServiceResourceTypes.DATASTORE, _to_datastore)

    def get_analysis_job_reports(self, job_id):
        """Get Reports output from (pbsmrtpipe) analysis job"""
        return self._get_job_resource_type_with_transform(JobTypes.PB_PIPE, job_id, ServiceResourceTypes.REPORTS, lambda x: x)

    def get_analysis_job_report_details(self, job_id, report_uuid):
        _d = dict(t=JobTypes.PB_PIPE, i=job_id, r=ServiceResourceTypes.REPORTS, p=ServiceAccessLayer.ROOT_JOBS, u=report_uuid)
        return _process_rget_or_none(lambda x: x)(_to_url(self.uri, "{p}/{t}/{i}/{r}/{u}".format(**_d)))

    def get_analysis_job_entry_points(self, job_id):
        return self._get_job_resource_type_with_transform(JobTypes.PB_PIPE, job_id, ServiceResourceTypes.ENTRY_POINTS, _to_entry_points)

    def get_import_dataset_job_datastore(self, job_id):
        """Get a List of Service DataStore files from an import DataSet job"""
        return self._get_job_resource_type(JobTypes.IMPORT_DS, job_id, ServiceResourceTypes.DATASTORE)

    def get_merge_dataset_job_datastore(self, job_id):
        return self._get_job_resource_type(JobTypes.MERGE_DS, job_id, ServiceResourceTypes.DATASTORE)

    def _import_dataset(self, dataset_type, path):
        # This returns a job resource
        url = self._to_url("{p}/{x}".format(x=JobTypes.IMPORT_DS, p=ServiceAccessLayer.ROOT_JOBS))
        return _import_dataset_by_type(dataset_type)(url, path)

    def run_import_dataset_by_type(self, dataset_type, path_to_xml):
        job_or_error = self._import_dataset(dataset_type, path_to_xml)
        custom_err_msg = "Import {d} {p}".format(p=path_to_xml, d=dataset_type)
        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self, job_id, sleep_time=self._sleep_time)

    def _run_import_and_block(self, func, path, time_out=None):
        # func while be self.import_dataset_X
        job_or_error = func(path)
        custom_err_msg = "Import {p}".format(p=path)
        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self, job_id, time_out=time_out,
                                          sleep_time=self._sleep_time)

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

    def run_import_local_dataset(self, path):
        """Import a file from FS that is local to where the services are running

        Returns a JobResult instance

        :rtype: JobResult
        """
        dataset_meta_type = get_dataset_metadata(path)
        result = self.get_dataset_by_uuid(dataset_meta_type.uuid)
        if result is None:
            log.info("Importing dataset {p}".format(p=path))
            return self.run_import_dataset_by_type(dataset_meta_type.metatype, path)
        else:
            log.debug("{f} already imported. Skipping importing. {r}".format(r=result, f=dataset_meta_type.metatype))
            # need to clean this up
            return JobResult(self.get_job_by_id(result['jobId']), 0, "")

    def get_dataset_by_uuid(self, int_or_uuid):
        """The recommend model is to look up DataSet type by explicit MetaType

        Returns None if the dataset was not found
        """
        return _process_rget_or_none(_null_func)(_to_url(self.uri, "{p}/{i}".format(i=int_or_uuid, p=ServiceAccessLayer.ROOT_DS)))

    def get_dataset_by_id(self, dataset_type, int_or_uuid):
        """Get a Dataset using the DataSetMetaType and (int|uuid) of the dataset"""
        ds_endpoint = _get_endpoint_or_raise(dataset_type)
        return _process_rget(_to_url(self.uri, "{p}/{t}/{i}".format(t=ds_endpoint, i=int_or_uuid, p=ServiceAccessLayer.ROOT_DS)))

    def _get_datasets_by_type(self, dstype):
        return _process_rget(_to_url(self.uri, "{p}/{i}".format(i=dstype, p=ServiceAccessLayer.ROOT_DS)))

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

    def get_ccsreadset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_CCS, int_or_uuid)

    def get_ccsreadsets(self):
        return self._get_datasets_by_type("ccsreads")

    def get_alignmentsets(self):
        return self._get_datasets_by_type("alignments")

    def import_fasta(self, fasta_path, name, organism, ploidy):
        """Convert fasta file to a ReferenceSet and Import. Returns a Job """
        d = dict(path=fasta_path,
                 name=name,
                 organism=organism,
                 ploidy=ploidy)
        return _process_rpost_with_transform(ServiceJob.from_d)(self._to_url("{p}/{t}".format(p=ServiceAccessLayer.ROOT_JOBS, t=JobTypes.CONVERT_FASTA)), d)

    def run_import_fasta(self, fasta_path, name, organism, ploidy, time_out=JOB_DEFAULT_TIMEOUT):
        """Import a Reference into a Block"""""
        job_or_error = self.import_fasta(fasta_path, name, organism, ploidy)
        _d = dict(f=fasta_path, n=name, o=organism, p=ploidy)
        custom_err_msg = "Fasta-convert path:{f} name:{n} organism:{o} ploidy:{p}".format(**_d)
        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self, job_id, time_out=time_out,
                                          sleep_time=self._sleep_time)

    def create_logger_resource(self, idx, name, description):
        _d = dict(id=idx, name=name, description=description)
        return _process_rpost(_to_url(self.uri, "/smrt-base/loggers"), _d)

    def log_progress_update(self, job_type_id, job_id, message, level, source_id):
        """This is the generic job logging mechanism"""
        _d = dict(message=message, level=level, sourceId=source_id)
        return _process_rpost(_to_url(self.uri, "{p}/{t}/{i}/log".format(t=job_type_id, i=job_id, p=ServiceAccessLayer.ROOT_JOBS)), _d)

    def get_pipeline_template_by_id(self, pipeline_template_id):
        return _process_rget(_to_url(self.uri, "{p}/{i}".format(i=pipeline_template_id, p=ServiceAccessLayer.ROOT_PT)))

    def create_by_pipeline_template_id(self, name, pipeline_template_id, epoints, task_options=()):
        """Creates and runs a pbsmrtpipe pipeline by pipeline template id"""
        # sanity checking to see if pipeline is valid
        _ = self.get_pipeline_template_by_id(pipeline_template_id)

        seps = [dict(entryId=e.entry_id, fileTypeId=e.dataset_type, datasetId=e.resource) for e in epoints]

        def _to_o(opt_id, opt_value, option_type_id):
            return dict(optionId=opt_id, value=opt_value, optionTypeId=option_type_id)

        task_options = list(task_options)
        # FIXME. Need to define this in the scenario IO layer.
        # workflow_options = [_to_o("woption_01", "value_01")]
        workflow_options = []
        d = dict(name=name, pipelineId=pipeline_template_id, entryPoints=seps, taskOptions=task_options, workflowOptions=workflow_options)
        raw_d = _process_rpost(_to_url(self.uri, "{r}/{p}".format(p=JobTypes.PB_PIPE, r=ServiceAccessLayer.ROOT_JOBS)), d)
        return ServiceJob.from_d(raw_d)

    def run_by_pipeline_template_id(self, name, pipeline_template_id, epoints, task_options=(), time_out=JOB_DEFAULT_TIMEOUT):
        """Blocks and runs a job with a timeout"""

        job_or_error = self.create_by_pipeline_template_id(name, pipeline_template_id, epoints, task_options=task_options)

        _d = dict(name=name, p=pipeline_template_id, eps=epoints)
        custom_err_msg = "Job {n} args: {a}".format(n=name, a=_d)

        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self, job_id, time_out=time_out,
                                          sleep_time=self._sleep_time)


def log_pbsmrtpipe_progress(total_url, message, level, source_id, ignore_errors=True):
    """Log the status of a pbsmrtpipe to SMRT Server"""

    # Need to clarify the model here. Trying to pass the most minimal
    # data necessary to pbsmrtpipe.
    _d = dict(message=message, level=level, sourceId=source_id)
    if ignore_errors:
        try:
            return _process_rpost(total_url, _d)
        except Exception as e:
            log.warn("Failed Request to {u} data: {d}. {e}".format(u=total_url, d=_d, e=e))
    else:
        return _process_rpost(total_url, _d)


def add_datastore_file(total_url, datastore_file, ignore_errors=True):
    """Add datastore to SMRT Server

    :type datastore_file: DataStoreFile
    """
    _d = datastore_file.to_dict()
    if ignore_errors:
        try:
            return _process_rpost(total_url, _d)
        except Exception as e:
            log.warn("Failed Request to {u} data: {d}. {e}".format(u=total_url, d=_d, e=e))
    else:
        return _process_rpost(total_url, _d)
