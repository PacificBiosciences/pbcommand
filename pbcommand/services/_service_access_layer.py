"""
Utils for Updating state/progress and results to WebServices
"""

import base64
import datetime
import json
import logging
import os
import pprint
import time
import warnings

import pytz
import requests
from requests import RequestException
# To disable the ssl cert check warning
from requests.packages.urllib3.exceptions import InsecureRequestWarning  # pylint: disable=import-error
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  # pylint: disable=no-member

from pbcommand.models import (FileTypes,
                              DataSetFileType,
                              DataStore,
                              DataStoreFile)
from pbcommand.utils import get_dataset_metadata
from .models import (SMRTServiceBaseError,
                     JobResult, JobStates, JobExeError, JobTypes,
                     ServiceResourceTypes, ServiceJob, JobEntryPoint,
                     JobTask)
from pbcommand.pb_io import load_report_from
from .utils import to_sal_summary


log = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# log.addHandler(logging.NullHandler())  # to prevent the annoying 'No
# handlers .. ' msg

# Everything else is considered a non-public
__all__ = [
    'ServiceAccessLayer',
    'SmrtLinkAuthClient',
]


class Constants:
    HEADERS = {'Content-type': 'application/json'}


def __jsonable_request(request_method, headers):
    def wrapper(url, d_):
        data = json.dumps(d_)
        # FIXME 'verify' should be passed in
        return request_method(url, data=data, headers=headers, verify=False)
    return wrapper


def _post_requests(headers):
    return __jsonable_request(requests.post, headers)


def _put_requests(headers):
    return __jsonable_request(requests.put, headers)


def _get_requests(headers):
    def wrapper(url):
        return requests.get(url, headers=headers, verify=False)
    return wrapper


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


def __get_headers(h):
    if h is None:
        return Constants.HEADERS
    return h


def _process_rget(total_url, ignore_errors=False, headers=None):
    """Process get request and return JSON response. Raise if not successful"""
    r = _get_requests(__get_headers(headers))(total_url)
    _parse_base_service_error(r)
    if not r.ok and not ignore_errors:
        log.error(
            "Failed ({s}) GET to {x}".format(
                x=total_url,
                s=r.status_code))
    r.raise_for_status()
    j = r.json()
    return j


def _process_rget_with_transform(func, ignore_errors=False):
    """Post process the JSON result (if successful) with F(json_d) -> T"""
    def wrapper(total_url, headers=None):
        j = _process_rget(
            total_url,
            ignore_errors=ignore_errors,
            headers=headers)
        return func(j)
    return wrapper


def _process_rget_with_jobs_transform(
        total_url, ignore_errors=False, headers=None):
    # defining an internal method, because this used in several places
    jobs_d = _process_rget(
        total_url,
        ignore_errors=ignore_errors,
        headers=headers)
    # Sort by Id desc so newer jobs show up first
    jobs = [ServiceJob.from_d(job_d) for job_d in jobs_d]
    return sorted(jobs, key=lambda x: x.id, reverse=True)


def _process_rget_or_none(func, ignore_errors=False):
    """
    apply the transform func to the output of GET request if it was successful, else returns None

    This is intended to be used for looking up Results by Id where the a 404
    is found.
    """
    def wrapper(total_url, headers):
        try:
            return _process_rget_with_transform(
                func, ignore_errors)(total_url, headers)
        except (RequestException, SMRTServiceBaseError):
            # FIXME
            # this should be a tighter exception case
            # only look for 404
            return None

    return wrapper


def _process_rget_with_job_transform_or_none(total_url, headers=None):
    return _process_rget_or_none(ServiceJob.from_d)(total_url, headers=headers)


def __process_creatable_to_json(f):
    def wrapper(total_url, payload_d, headers):
        r = f(__get_headers(headers))(total_url, payload_d)
        _parse_base_service_error(r)
        # FIXME This should be strict to only return a 201
        if r.status_code not in (200, 201, 202, 204):
            log.error(
                "Failed ({s} to call {u}".format(
                    u=total_url,
                    s=r.status_code))
            log.error("payload")
            log.error("\n" + pprint.pformat(payload_d))
        r.raise_for_status()
        j = r.json()
        return j
    return wrapper


_process_rpost = __process_creatable_to_json(_post_requests)
_process_rput = __process_creatable_to_json(_put_requests)


def _process_rpost_with_transform(func):
    def wrapper(total_url, payload_d, headers=None):
        j = _process_rpost(total_url, payload_d, headers)
        return func(j)
    return wrapper


def _process_rput_with_transform(func):
    def wrapper(total_url, payload_d, headers=None):
        j = _process_rput(total_url, payload_d, headers)
        return func(j)
    return wrapper


def _to_url(base, ext):
    return "".join([base, ext])


def _null_func(x):
    # Pass thorough func
    return x


def _transform_job_tasks(j):
    return [JobTask.from_d(d) for d in j]


def _get_job_by_id_or_raise(sal, job_id, error_klass,
                            error_messge_extras=None):
    job = sal.get_job_by_id(job_id)

    if job is None:
        details = "" if error_messge_extras is None else error_messge_extras
        base_msg = "Failed to find job {i}".format(i=job_id)
        emsg = " ".join([base_msg, details])
        raise error_klass(emsg)

    return job


def _block_for_job_to_complete(sal, job_id, time_out=1200, sleep_time=2,
                               abort_on_interrupt=True,
                               retry_on_failure=False):
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

    try:
        external_job_id = None
        time.sleep(sleep_time)
        job = _get_job_by_id_or_raise(sal, job_id, KeyError)
        log.info("SMRT Link job {i} ({u})".format(i=job.id, u=job.uuid))
        log.debug("time_out = {t}".format(t=time_out))

        error_msg = ""
        job_result = JobResult(job, 0, error_msg)
        started_at = time.time()

        # number of polling steps
        i = 0
        while True:
            run_time = time.time() - started_at
            if external_job_id is None and job.external_job_id is not None:
                external_job_id = job.external_job_id
                log.info("Cromwell workflow ID is %s", external_job_id)

            if job.state in JobStates.ALL_COMPLETED:
                break

            i += 1
            time.sleep(sleep_time)

            msg = "Running pipeline {n} (job {j}) state: {s} runtime:{r:.2f} sec {i} iteration".format(
                n=job.name, j=job.id, s=job.state, r=run_time, i=i)
            log.debug(msg)
            # making the exceptions different to distinguish between an initial
            # error and a "polling" error. Adding some msg details
            # FIXME this should distinguish between failure modes - an HTTP 503
            # or 401 is different from a 404 in this context
            try:
                job = _get_job_by_id_or_raise(
                    sal, job_id, JobExeError, error_messge_extras=msg)
            except JobExeError as e:
                if retry_on_failure:
                    log.error(e)
                    log.warn("Polling job {i} failed".format(i=job_id))
                    continue
                else:
                    raise

            # FIXME, there's currently not a good way to get errors for jobs
            job_result = JobResult(job, run_time, "")
            if time_out is not None:
                if run_time > time_out:
                    raise JobExeError(
                        "Exceeded runtime {r} of {t}. {m}".format(
                            r=run_time, t=time_out, m=msg))

        return job_result
    except KeyboardInterrupt:
        if abort_on_interrupt:
            sal.terminate_job_id(job_id)
        raise


# Make this consistent somehow. Maybe defined 'shortname' in the core model?
# Martin is doing this for the XML file names
DATASET_METATYPES_TO_ENDPOINTS = {
    FileTypes.DS_SUBREADS_H5.file_type_id: "hdfsubreads",
    FileTypes.DS_SUBREADS.file_type_id: "subreads",
    FileTypes.DS_ALIGN.file_type_id: "alignments",
    FileTypes.DS_REF.file_type_id: "references",
    FileTypes.DS_BARCODE.file_type_id: "barcodes",
    FileTypes.DS_CCS.file_type_id: "ccsreads",
    FileTypes.DS_CONTIG.file_type_id: "contigs",
    FileTypes.DS_ALIGN_CCS.file_type_id: "cssalignments",
    FileTypes.DS_GMAP_REF.file_type_id: "gmapreferences"}


def _get_endpoint_or_raise(ds_type):
    if ds_type in DATASET_METATYPES_TO_ENDPOINTS:
        return DATASET_METATYPES_TO_ENDPOINTS[ds_type]
    raise KeyError("Unsupported datasettype {t}. Supported values {v}".format(
        t=ds_type, v=list(DATASET_METATYPES_TO_ENDPOINTS.keys())))


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
        raise JobExeError(
            "Failed to create job. {e}. Raw Response {x}".format(
                e=emsg, x=job_or_error))


def _to_ds_file(d):
    # is_chunk this isn't exposed at the service level
    return DataStoreFile(d['uuid'], d['sourceId'], d['fileTypeId'], d['path'],
                         is_chunked=False, name=d.get("name", ""), description=d.get("description", ""))


def _to_datastore(dx):
    # Friction to get around service endpoint not returning a list of files
    ds_files = [_to_ds_file(d) for d in dx]
    return DataStore(ds_files)


def _to_job_report_files(dx):
    return [{u"reportTypeId": d["reportTypeId"],
             u"dataStoreFile": _to_ds_file(d["dataStoreFile"])} for d in dx]


def _to_entry_points(d):
    return [JobEntryPoint.from_d(i) for i in d]


def _get_all_report_attributes(
        sal_get_reports_func, sal_get_reports_details_func, job_id):
    """Util func for getting report Attributes

    Note, this assumes that only one report type has been created. This is
    probably not a great idea. Should re-evaluate this.
    """
    report_datafiles = sal_get_reports_func(job_id)
    report_uuids = [list(r.values())[0].uuid for r in report_datafiles]
    reports = [
        sal_get_reports_details_func(
            job_id,
            r_uuid) for r_uuid in report_uuids]
    all_report_attributes = {}

    for r in reports:
        for x in r['attributes']:
            all_report_attributes[x['id']] = x['value']

    return all_report_attributes


def _to_relative_tasks_url(job_type):
    def wrapper(job_id_or_uuid):
        return "/".join([ServiceAccessLayer.ROOT_JOBS,
                         job_type, str(job_id_or_uuid), "tasks"])
    return wrapper


def _show_deprecation_warning(msg):
    if "PB_TEST_MODE" not in os.environ:
        warnings.simplefilter('once', DeprecationWarning)
        warnings.warn(msg, DeprecationWarning)
        warnings.simplefilter('default', DeprecationWarning)  # reset filte


class ServiceAccessLayer:  # pragma: no cover
    """
    General Client Access Layer for interfacing with the job types on
    SMRT Link Analysis Services.  This API only supports insecure (HTTP)
    access to localhost.

    As of 10-02-2018, this should only be used (minimally) for internal purposes. All
    access to the Services should be done via SmrtLinkAuthClient.
    """

    ROOT_SL = "/smrt-link"
    ROOT_JM = ROOT_SL + "/job-manager"
    ROOT_JOBS = ROOT_JM + "/jobs"
    ROOT_MJOBS = ROOT_JM + "/multi-jobs"
    ROOT_RUNS = ROOT_SL + "/runs"
    ROOT_SAMPLES = ROOT_SL + "/samples"
    ROOT_DS = "/smrt-link/datasets"
    ROOT_PT = '/smrt-link/resolved-pipeline-templates'

    # in sec when blocking to run a job
    JOB_DEFAULT_TIMEOUT = 60 * 30

    def __init__(self, base_url, port, debug=False, sleep_time=2):
        """

        :param base_url: base url of the SL Server.  This MUST be either 'localhost' or 'http://localhost'
        :param port: port of the SL server
        :param debug: set improved debugging output on Services request failures
        :param sleep_time: sleep time (in seconds) between polling for job status
        """
        self.base_url = self._to_base_url(base_url)
        self.port = port
        # This will display verbose details with respect to the failed request
        self.debug = debug
        self._sleep_time = sleep_time

        if self.__class__.__name__ == "ServiceAccessLayer":
            _show_deprecation_warning(
                "Please use the SmrtLinkAuthClient', direct localhost access is not publicly supported")

    def _get_headers(self):
        return Constants.HEADERS

    def _to_base_url(self, h):
        if h not in {"http://localhost", "localhost"}:
            raise NotImplementedError(
                "This API only supports HTTP connections to localhost")
        prefix = "http://"
        return h if h.startswith(prefix) else prefix + h

    @property
    def uri(self):
        return "{b}:{u}".format(b=self.base_url, u=self.port)

    def _to_url(self, rest):
        return _to_url(self.uri, rest)

    def __repr__(self):
        return "<{k} {u} >".format(k=self.__class__.__name__, u=self.uri)

    def to_summary(self):
        """
        Returns a summary of System status, DataSets, and Jobs in the system

        :rtype: str
        """
        return to_sal_summary(self)

    def get_status(self):
        """Get status of the server

        :rtype: dict
        """
        # This should be converted to a concrete typed object
        return _process_rget(_to_url(self.uri, "/status"),
                             headers=self._get_headers())

    def get_job_by_type_and_id(self, job_type, job_id):
        return _process_rget_with_job_transform_or_none(_to_url(self.uri, "{p}/{t}/{i}".format(
            i=job_id, t=job_type, p=ServiceAccessLayer.ROOT_JOBS)), headers=self._get_headers())

    def get_job_by_id(self, job_id):
        """Get a Job by int id"""
        # FIXME. Make this an internal method It's ambiguous which job type
        # type you're asking for
        return _process_rget_with_job_transform_or_none(_to_url(
            self.uri, "{r}/{i}".format(i=job_id, r=ServiceAccessLayer.ROOT_JOBS)), headers=self._get_headers())

    def _get_job_resource_type(self, job_type, job_id, resource_type_id):
        # grab the datastore or the reports
        _d = dict(
            t=job_type,
            i=job_id,
            r=resource_type_id,
            p=ServiceAccessLayer.ROOT_JOBS)
        return _process_rget_with_job_transform_or_none(
            _to_url(self.uri, "{p}/{t}/{i}/{r}".format(**_d)), headers=self._get_headers())

    def _get_job_resource_type_with_transform(
            self, job_type, job_id, resource_type_id, transform_func):
        _d = dict(
            t=job_type,
            i=job_id,
            r=resource_type_id,
            p=ServiceAccessLayer.ROOT_JOBS)
        return _process_rget_or_none(transform_func)(
            _to_url(self.uri, "{p}/{t}/{i}/{r}".format(**_d)), headers=self._get_headers())

    def _get_jobs_by_job_type(self, job_type, query=None):
        base_url = "{p}/{t}".format(t=job_type, p=ServiceAccessLayer.ROOT_JOBS)
        if query is not None:
            base_url = "".join([base_url, "?", query])
        return _process_rget_with_jobs_transform(_to_url(self.uri, base_url),
                                                 headers=self._get_headers())

    def get_multi_analysis_jobs(self):
        return _process_rget_with_jobs_transform(_to_url(self.uri, "{p}/{t}".format(
            t="multi-analysis", p=ServiceAccessLayer.ROOT_MJOBS)), headers=self._get_headers())

    def get_multi_analysis_job_by_id(self, int_or_uuid):
        return _process_rget_with_job_transform_or_none(_to_url(self.uri, "{p}/{t}/{i}".format(
            t="multi-analysis", p=ServiceAccessLayer.ROOT_MJOBS, i=int_or_uuid)), headers=self._get_headers())

    def get_multi_analysis_job_children_by_id(self, multi_job_int_or_uuid):
        return _process_rget_with_jobs_transform(
            _to_url(self.uri,
                    "{p}/{t}/{i}/jobs".format(t="multi-analysis",
                                              p=ServiceAccessLayer.ROOT_MJOBS,
                                              i=multi_job_int_or_uuid)),
            headers=self._get_headers())

    def get_all_analysis_jobs(self):
        return _process_rget_with_jobs_transform(
            _to_url(self.uri, "{p}/analysis-jobs".format(
                p=ServiceAccessLayer.ROOT_JM)),
            headers=self._get_headers())

    def get_analysis_jobs(self, query=None):
        return self._get_jobs_by_job_type(JobTypes.ANALYSIS, query=query)

    def get_pbsmrtpipe_jobs(self, query=None):
        """:rtype: list[ServiceJob]"""
        _show_deprecation_warning("Please use get_analysis_jobs() instead")
        return self.get_analysis_jobs(query=query)

    def get_cromwell_jobs(self):
        """:rtype: list[ServiceJob]"""
        return self._get_jobs_by_job_type(JobTypes.CROMWELL)

    def get_import_dataset_jobs(self):
        """:rtype: list[ServiceJob]"""
        return self._get_jobs_by_job_type(JobTypes.IMPORT_DS)

    def get_merge_dataset_jobs(self):
        """:rtype: list[ServiceJob]"""
        return self._get_jobs_by_job_type(JobTypes.MERGE_DS)

    def get_fasta_convert_jobs(self):
        """:rtype: list[ServiceJob]"""
        self._get_jobs_by_job_type(JobTypes.CONVERT_FASTA)

    def get_analysis_job_by_id(self, job_id):
        """Get an Analysis job by id or UUID or return None

        :rtype: ServiceJob
        """
        return self.get_job_by_type_and_id(JobTypes.ANALYSIS, job_id)

    def get_import_job_by_id(self, job_id):
        return self.get_job_by_type_and_id(JobTypes.IMPORT_DS, job_id)

    def get_analysis_job_datastore(self, job_id):
        """Get DataStore output from (pbsmrtpipe) analysis job"""
        # this doesn't work the list is sli
        return self._get_job_resource_type_with_transform(
            "pbsmrtpipe", job_id, ServiceResourceTypes.DATASTORE, _to_datastore)

    def _to_dsf_id_url(self, job_id, dsf_uuid):
        u = "/".join([ServiceAccessLayer.ROOT_JOBS, "pbsmrtpipe",
                      str(job_id), ServiceResourceTypes.DATASTORE, dsf_uuid])
        return _to_url(self.uri, u)

    def get_analysis_job_datastore_file(self, job_id, dsf_uuid):
        return _process_rget_or_none(_to_ds_file)(
            self._to_dsf_id_url(job_id, dsf_uuid), headers=self._get_headers())

    def get_analysis_job_datastore_file_download(
            self, job_id, dsf_uuid, output_file=None):
        """
        Download an DataStore file to an output file

        :param job_id:
        :param dsf_uuid:
        :param output_file: if None, the file name from the server (content-disposition) will be used.
        :return:
        """
        url = "{}/download".format(self._to_dsf_id_url(job_id, dsf_uuid))
        dsf = self.get_analysis_job_datastore_file(job_id, dsf_uuid)

        default_name = "download-job-{}-dsf-{}".format(job_id, dsf_uuid)

        if dsf is not None:
            r = requests.get(
                url,
                stream=True,
                verify=False,
                headers=self._get_headers())
            if output_file is None:
                try:
                    # 'attachment; filename="job-106-be2b5106-91dc-4ef9-b199-f1481f88b7e4-file-024.subreadset.xml'
                    raw_header = r.headers.get('content-disposition')
                    local_filename = raw_header.split(
                        "filename=")[-1].replace('"', '')
                except (TypeError, IndexError, KeyError, AttributeError):
                    local_filename = default_name

            else:
                local_filename = output_file

            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
            r.close()
            return local_filename
        else:
            # This should probably return None to be consistent with the
            # current API
            raise KeyError(
                "Unable to get DataStore file {} from Job {}".format(
                    dsf_uuid, job_id))

    def get_analysis_job_reports(self, job_id):
        """Get list of DataStore ReportFile types output from (pbsmrtpipe) analysis job"""
        return self._get_job_resource_type_with_transform(
            JobTypes.ANALYSIS, job_id, ServiceResourceTypes.REPORTS, _to_job_report_files)

    def get_analysis_job_reports_objs(self, job_id):
        """
        Get a List of Report Instances

        :param job_id:
        :rtype list[Report]
        :return: List of Reports
        """
        job_reports = self.get_analysis_job_reports(job_id)
        return [self.get_analysis_job_report_obj(
            job_id, x['dataStoreFile'].uuid) for x in job_reports]

    def __get_report_d(self, job_id, report_uuid, processor_func):
        _d = dict(t=JobTypes.ANALYSIS, i=job_id, r=ServiceResourceTypes.REPORTS, p=ServiceAccessLayer.ROOT_JOBS,
                  u=report_uuid)
        u = "{p}/{t}/{i}/{r}/{u}".format(**_d)
        return _process_rget_or_none(processor_func)(
            _to_url(self.uri, u), headers=self._get_headers())

    def get_analysis_job_report_details(self, job_id, report_uuid):
        return self.__get_report_d(job_id, report_uuid, lambda x: x)

    def get_analysis_job_report_obj(self, job_id, report_uuid):
        """
        Fetch a SMRT Link Report Instance from a Job Id and Report UUID

        There's inconsistencies in the API, hence the naming of the method is a bit verbose.
        :rtype Report
        """
        return self.__get_report_d(job_id, report_uuid, load_report_from)

    def get_analysis_job_report_attrs(self, job_id):
        """Return a dict of all the Report Attributes"""
        return _get_all_report_attributes(
            self.get_analysis_job_reports, self.get_analysis_job_report_details, job_id)

    def get_import_job_reports(self, job_id):
        return self._get_job_resource_type_with_transform(
            JobTypes.IMPORT_DS, job_id, ServiceResourceTypes.REPORTS, _to_job_report_files)

    def get_import_job_report_details(self, job_id, report_uuid):
        # It would have been better to return a Report instance, not raw json
        _d = dict(
            t=JobTypes.IMPORT_DS,
            i=job_id,
            r=ServiceResourceTypes.REPORTS,
            p=ServiceAccessLayer.ROOT_JOBS,
            u=report_uuid)
        return _process_rget_or_none(lambda x: x)(_to_url(
            self.uri, "{p}/{t}/{i}/{r}/{u}".format(**_d)), headers=self._get_headers())

    def get_import_job_report_attrs(self, job_id):
        """Return a dict of all the Report Attributes"""
        return _get_all_report_attributes(
            self.get_import_job_reports, self.get_import_job_report_details, job_id)

    def get_analysis_job_entry_points(self, job_id):
        return self._get_job_resource_type_with_transform(
            JobTypes.ANALYSIS, job_id, ServiceResourceTypes.ENTRY_POINTS, _to_entry_points)

    def get_import_dataset_job_datastore(self, job_id):
        """Get a List of Service DataStore files from an import DataSet job"""
        return self._get_job_resource_type(
            JobTypes.IMPORT_DS, job_id, ServiceResourceTypes.DATASTORE)

    def get_merge_dataset_job_datastore(self, job_id):
        return self._get_job_resource_type(
            JobTypes.MERGE_DS, job_id, ServiceResourceTypes.DATASTORE)

    def import_dataset(self,
                       path,
                       avoid_duplicate_import=False):
        # This returns a job resource
        url = self._to_url(
            "{p}/{x}".format(x=JobTypes.IMPORT_DS, p=ServiceAccessLayer.ROOT_JOBS))
        d = {
            "path": path,
            "avoidDuplicateImport": avoid_duplicate_import
        }
        return _process_rpost_with_transform(
            ServiceJob.from_d)(url, d, headers=self._get_headers())

    def run_import_dataset(self,
                           path_to_xml,
                           avoid_duplicate_import=False):
        job_or_error = self.import_dataset(
            path_to_xml,
            avoid_duplicate_import=avoid_duplicate_import)
        custom_err_msg = "Import {p}".format(p=path_to_xml)
        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(
            self, job_id, sleep_time=self._sleep_time)

    def run_import_dataset_by_type(self, dataset_type, path_to_xml,
                                   avoid_duplicate_import=False):
        return self.run_import_dataset(path_to_xml, avoid_duplicate_import)

    def _run_import_and_block(self, func, path, time_out=None):
        # func while be self.import_dataset_X
        job_or_error = func(path)
        custom_err_msg = "Import {p}".format(p=path)
        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self, job_id, time_out=time_out,
                                          sleep_time=self._sleep_time)

    def import_dataset_subread(self, path):
        log.warn("DEPRECATED METHOD")
        return self.import_dataset(path)

    def run_import_dataset_subread(self, path, time_out=10):
        return self._run_import_and_block(
            self.import_dataset_subread, path, time_out=time_out)

    def import_dataset_hdfsubread(self, path):
        log.warn("DEPRECATED METHOD")
        return self.import_dataset(path)

    def run_import_dataset_hdfsubread(self, path, time_out=10):
        return self._run_import_and_block(
            self.import_dataset_hdfsubread, path, time_out=time_out)

    def import_dataset_reference(self, path):
        log.warn("DEPRECATED METHOD")
        return self.import_dataset(path)

    def run_import_dataset_reference(self, path, time_out=10):
        return self._run_import_and_block(
            self.import_dataset_reference, path, time_out=time_out)

    def import_dataset_barcode(self, path):
        log.warn("DEPRECATED METHOD")
        return self.import_dataset(path)

    def run_import_dataset_barcode(self, path, time_out=10):
        return self._run_import_and_block(
            self.import_dataset_barcode, path, time_out=time_out)

    def run_import_local_dataset(self, path, avoid_duplicate_import=False):
        """Import a file from FS that is local to where the services are running

        Returns a JobResult instance

        :rtype: JobResult
        """
        dataset_meta_type = get_dataset_metadata(path)

        result = self.get_dataset_by_uuid(dataset_meta_type.uuid,
                                          ignore_errors=True)
        if result is None:
            log.info("Importing dataset {p}".format(p=path))
            job_result = self.run_import_dataset_by_type(
                dataset_meta_type.metatype, path, avoid_duplicate_import=avoid_duplicate_import)
            log.info("Confirming database update")
            # validation 1: attempt to retrieve dataset info
            result_new = self.get_dataset_by_uuid(dataset_meta_type.uuid)
            if result_new is None:
                raise JobExeError(("Dataset {u} was imported but could " +
                                   "not be retrieved; this may indicate " +
                                   "XML schema errors.").format(
                    u=dataset_meta_type.uuid))
            return job_result
        else:
            log.info(
                "{f} already imported. Skipping importing. {r}".format(
                    r=result, f=dataset_meta_type.metatype))
            # need to clean this up
            return JobResult(self.get_job_by_id(result['jobId']), 0, "")

    def get_dataset_children_jobs(self, dataset_id):
        """
        Get a List of Children Jobs for the DataSet

        :param dataset_id: DataSet Int or UUID
        :type dataset_id: int | string
        :rtype list[ServiceJob]
        """
        return _process_rget_with_jobs_transform(
            _to_url(self.uri, "{t}/datasets/{i}/jobs".format(t=ServiceAccessLayer.ROOT_SL, i=dataset_id)), headers=self._get_headers())

    def get_job_types(self):
        u = _to_url(
            self.uri, "{}/{}".format(ServiceAccessLayer.ROOT_JM, "job-types"))
        return _process_rget(u, headers=self._get_headers())

    def get_dataset_types(self):
        """Get a List of DataSet Types"""
        u = _to_url(self.uri,
                    "{}/{}".format(ServiceAccessLayer.ROOT_SL,
                                   "dataset-types"))
        return _process_rget(u, headers=self._get_headers())

    def get_dataset_by_uuid(self, int_or_uuid, ignore_errors=False):
        """The recommend model is to look up DataSet type by explicit MetaType

        Returns None if the dataset was not found
        """
        return _process_rget_or_none(_null_func, ignore_errors=ignore_errors)(
            _to_url(self.uri, "{p}/{i}".format(i=int_or_uuid,
                                               p=ServiceAccessLayer.ROOT_DS)),
            headers=self._get_headers())

    def get_dataset_by_id(self, dataset_type, int_or_uuid):
        """Get a Dataset using the DataSetMetaType and (int|uuid) of the dataset"""
        ds_endpoint = _get_endpoint_or_raise(dataset_type)
        return _process_rget(_to_url(self.uri, "{p}/{t}/{i}".format(
            t=ds_endpoint, i=int_or_uuid, p=ServiceAccessLayer.ROOT_DS)), headers=self._get_headers())

    def _get_dataset_details_by_id(self, dataset_type, int_or_uuid):
        """
        Get a Dataset Details (XML converted to JSON via webservices
        using the DataSetMetaType and (int|uuid) of the dataset
        """
        # FIXME There's some inconsistencies in the interfaces with regards to
        # returning None or raising
        ds_endpoint = _get_endpoint_or_raise(dataset_type)
        return _process_rget(_to_url(self.uri, "{p}/{t}/{i}/details".format(
            t=ds_endpoint, i=int_or_uuid, p=ServiceAccessLayer.ROOT_DS)), headers=self._get_headers())

    def _get_datasets_by_type(self, dstype):
        return _process_rget(_to_url(self.uri, "{p}/{i}".format(
            i=dstype, p=ServiceAccessLayer.ROOT_DS)), headers=self._get_headers())

    def get_subreadset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_SUBREADS, int_or_uuid)

    def get_subreadset_details_by_id(self, int_or_uuid):
        return self._get_dataset_details_by_id(
            FileTypes.DS_SUBREADS, int_or_uuid)

    def get_subreadsets(self):
        return self._get_datasets_by_type("subreads")

    def get_hdfsubreadset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_SUBREADS_H5, int_or_uuid)

    def get_hdfsubreadset_details_by_id(self, int_or_uuid):
        return self._get_dataset_details_by_id(
            FileTypes.DS_SUBREADS_H5, int_or_uuid)

    def get_hdfsubreadsets(self):
        return self._get_datasets_by_type("hdfsubreads")

    def get_referenceset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_REF, int_or_uuid)

    def get_referenceset_details_by_id(self, int_or_uuid):
        return self._get_dataset_details_by_id(FileTypes.DS_REF, int_or_uuid)

    def get_referencesets(self):
        return self._get_datasets_by_type("references")

    def get_barcodeset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_BARCODE, int_or_uuid)

    def get_barcodeset_details_by_id(self, int_or_uuid):
        return self._get_dataset_details_by_id(
            FileTypes.DS_BARCODE, int_or_uuid)

    def get_barcodesets(self):
        return self._get_datasets_by_type("barcodes")

    def get_alignmentset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_ALIGN, int_or_uuid)

    def get_alignmentset_details_by_id(self, int_or_uuid):
        return self._get_dataset_details_by_id(FileTypes.DS_ALIGN, int_or_uuid)

    def get_ccsreadset_by_id(self, int_or_uuid):
        return self.get_dataset_by_id(FileTypes.DS_CCS, int_or_uuid)

    def get_ccsreadset_details_by_id(self, int_or_uuid):
        return self._get_dataset_details_by_id(FileTypes.DS_CCS, int_or_uuid)

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
        return _process_rpost_with_transform(ServiceJob.from_d)(self._to_url(
            "{p}/{t}".format(p=ServiceAccessLayer.ROOT_JOBS, t=JobTypes.CONVERT_FASTA)), d, headers=self._get_headers())

    def run_import_fasta(self, fasta_path, name, organism,
                         ploidy, time_out=JOB_DEFAULT_TIMEOUT):
        """Import a Reference into a Block"""""
        job_or_error = self.import_fasta(fasta_path, name, organism, ploidy)
        _d = dict(f=fasta_path, n=name, o=organism, p=ploidy)
        custom_err_msg = "Fasta-convert path:{f} name:{n} organism:{o} ploidy:{p}".format(
            **_d)
        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self, job_id, time_out=time_out,
                                          sleep_time=self._sleep_time)

    def create_logger_resource(self, idx, name, description):
        _d = dict(id=idx, name=name, description=description)
        return _process_rpost(
            _to_url(self.uri, "/smrt-base/loggers"), _d, headers=self._get_headers())

    def log_progress_update(self, job_type_id, job_id,
                            message, level, source_id):
        """This is the generic job logging mechanism"""
        _d = dict(message=message, level=level, sourceId=source_id)
        return _process_rpost(_to_url(self.uri, "{p}/{t}/{i}/log".format(
            t=job_type_id, i=job_id, p=ServiceAccessLayer.ROOT_JOBS)), _d, headers=self._get_headers())

    def get_pipeline_template_by_id(self, pipeline_template_id):
        return _process_rget(_to_url(self.uri, "{p}/{i}".format(
            i=pipeline_template_id, p=ServiceAccessLayer.ROOT_PT)), headers=self._get_headers())

    def create_by_pipeline_template_id(self,
                                       name,
                                       pipeline_template_id,
                                       epoints,
                                       task_options=(),
                                       workflow_options=(),
                                       tags=()):
        """Creates and runs a pbsmrtpipe pipeline by pipeline template id


        :param tags: Tags should be a set of strings
        """
        if pipeline_template_id.startswith("pbsmrtpipe"):
            raise NotImplementedError("pbsmrtpipe is no longer supported")

        # sanity checking to see if pipeline is valid
        _ = self.get_pipeline_template_by_id(pipeline_template_id)

        seps = [
            dict(
                entryId=e.entry_id,
                fileTypeId=e.dataset_type,
                datasetId=e.resource) for e in epoints]

        def _to_o(opt_id, opt_value, option_type_id):
            return dict(optionId=opt_id, value=opt_value,
                        optionTypeId=option_type_id)

        task_options = list(task_options)
        d = dict(name=name,
                 pipelineId=pipeline_template_id,
                 entryPoints=seps,
                 taskOptions=task_options,
                 workflowOptions=workflow_options)

        # Only add the request if the non empty.
        if tags:
            tags_str = ",".join(list(tags))
            d['tags'] = tags_str
        job_type = JobTypes.ANALYSIS
        raw_d = _process_rpost(_to_url(self.uri,
                                       "{r}/{p}".format(p=job_type,
                                                        r=ServiceAccessLayer.ROOT_JOBS)),
                               d,
                               headers=self._get_headers())
        return ServiceJob.from_d(raw_d)

    def run_by_pipeline_template_id(self,
                                    name,
                                    pipeline_template_id,
                                    epoints,
                                    task_options=(),
                                    workflow_options=(),
                                    time_out=JOB_DEFAULT_TIMEOUT,
                                    tags=(),
                                    abort_on_interrupt=True,
                                    retry_on_failure=False):
        """Blocks and runs a job with a timeout"""

        job_or_error = self.create_by_pipeline_template_id(
            name,
            pipeline_template_id,
            epoints,
            task_options=task_options,
            workflow_options=workflow_options,
            tags=tags)

        _d = dict(name=name, p=pipeline_template_id, eps=epoints)
        custom_err_msg = "Job {n} args: {a}".format(n=name, a=_d)

        job_id = _job_id_or_error(job_or_error, custom_err_msg=custom_err_msg)
        return _block_for_job_to_complete(self,
                                          job_id,
                                          time_out=time_out,
                                          sleep_time=self._sleep_time,
                                          abort_on_interrupt=abort_on_interrupt,
                                          retry_on_failure=retry_on_failure)

    def run_cromwell_workflow(self,
                              name,
                              workflow_source,
                              inputs_json,
                              engine_options,
                              dependencies_zip,
                              time_out=JOB_DEFAULT_TIMEOUT,
                              tags=(),
                              abort_on_interrupt=True):
        d = dict(
            name=name,
            workflowSource=workflow_source,
            inputsJson=inputs_json,
            engineOptions=engine_options,
            dependenciesZip=dependencies_zip)
        if tags:
            tags_str = ",".join(list(tags))
            d['tags'] = tags_str
        raw_d = _process_rpost(_to_url(self.uri,
                                       "{r}/{p}".format(p=JobTypes.CROMWELL,
                                                        r=ServiceAccessLayer.ROOT_JOBS)),
                               d,
                               headers=self._get_headers())
        job = ServiceJob.from_d(raw_d)
        return _block_for_job_to_complete(self, job.id, time_out=time_out,
                                          sleep_time=self._sleep_time,
                                          abort_on_interrupt=abort_on_interrupt)

    def terminate_job(self, job):
        """
        POST a terminate request appropriate to the job type.  Currently only
        supported for pbsmrtpipe, cromwell, and analysis job types.
        """
        log.warn("Terminating job {i} ({u})".format(i=job.id, u=job.uuid))
        if job.external_job_id is not None:
            log.warn("Will abort Cromwell workflow %s", job.external_job_id)
        return _process_rpost(
            _to_url(self.uri, "{r}/{p}/{i}/terminate".format(
                p=job.job_type,
                r=ServiceAccessLayer.ROOT_JOBS,
                i=job.id)),
            {},
            headers=self._get_headers())

    def terminate_job_id(self, job_id):
        job = _get_job_by_id_or_raise(self, job_id, KeyError)
        return self.terminate_job(job)

    def resume_job(self,
                   job_id,
                   time_out=JOB_DEFAULT_TIMEOUT,
                   abort_on_interrupt=True):
        job = _get_job_by_id_or_raise(self, job_id, KeyError)
        if job.state in JobStates.ALL_COMPLETED:
            return JobResult(job, 0, "")
        return _block_for_job_to_complete(self, job.id, time_out=time_out,
                                          sleep_time=self._sleep_time,
                                          abort_on_interrupt=abort_on_interrupt)

    def get_analysis_job_tasks(self, job_id_or_uuid):
        """Get all the Task associated with a Job by UUID or Int Id"""
        job_url = self._to_url(
            _to_relative_tasks_url(
                JobTypes.ANALYSIS)(job_id_or_uuid))
        return _process_rget_with_transform(_transform_job_tasks)(
            job_url, headers=self._get_headers())

    def get_import_job_tasks(self, job_id_or_uuid):
        # this is more for testing purposes
        job_url = self._to_url(
            _to_relative_tasks_url(
                JobTypes.IMPORT_DS)(job_id_or_uuid))
        return _process_rget_with_transform(_transform_job_tasks)(
            job_url, headers=self._get_headers())

    def get_manifests(self):
        u = self._to_url("{}/manifests".format(ServiceAccessLayer.ROOT_SL))
        return _process_rget_with_transform(
            _null_func)(u, headers=self._get_headers())

    def get_manifest_by_id(self, ix):
        u = self._to_url(
            "{}/manifests/{}".format(ServiceAccessLayer.ROOT_SL, ix))
        return _process_rget_or_none(_null_func)(
            u, headers=self._get_headers())

    def get_runs(self):
        u = self._to_url("{}".format(ServiceAccessLayer.ROOT_RUNS))
        return _process_rget_with_transform(
            _null_func)(u, headers=self._get_headers())

    def get_run_details(self, run_uuid):
        u = self._to_url(
            "{}/{}".format(ServiceAccessLayer.ROOT_RUNS, run_uuid))
        return _process_rget_or_none(_null_func)(
            u, headers=self._get_headers())

    def get_run_collections(self, run_uuid):
        u = self._to_url(
            "{}/{}/collections".format(ServiceAccessLayer.ROOT_RUNS, run_uuid))
        return _process_rget_with_transform(
            _null_func)(u, headers=self._get_headers())

    def get_run_collection(self, run_uuid, collection_uuid):
        u = self._to_url(
            "{}/{}/collections/{}".format(ServiceAccessLayer.ROOT_RUNS, run_uuid, collection_uuid))
        return _process_rget_or_none(_null_func)(
            u, headers=self._get_headers())

    def get_samples(self):
        u = self._to_url("{}/samples".format(ServiceAccessLayer.ROOT_SL, ))
        return _process_rget_with_transform(
            _null_func)(u, headers=self._get_headers())

    def get_sample_by_id(self, sample_uuid):
        u = self._to_url(
            "{}/samples/{}".format(ServiceAccessLayer.ROOT_SL, sample_uuid))
        return _process_rget_or_none(_null_func)(
            u, headers=self._get_headers())

    def submit_multi_job(self, job_options):
        u = self._to_url(
            "{}/multi-analysis".format(ServiceAccessLayer.ROOT_MJOBS))
        return _process_rpost_with_transform(ServiceJob.from_d)(
            u, job_options, headers=self._get_headers())


def __run_and_ignore_errors(f, warn_message):
    """
    Black hole ignoring exceptions from a func with no-args and
    logging the error has a warning.
    """
    try:
        return f()
    except Exception as e:
        log.warn(warn_message + " {e}".format(e=e))


def _run_func(f, warn_message, ignore_errors=True):
    if ignore_errors:
        return __run_and_ignore_errors(f, warn_message)
    else:
        return f()


def log_pbsmrtpipe_progress(total_url, message, level, source_id, ignore_errors=True, headers=None):  # pragma: no cover
    """Log the status of a pbsmrtpipe to SMRT Server"""
    # Keeping this as public to avoid breaking pbsmrtpipe. The
    # new public interface should be the JobServiceClient

    # Need to clarify the model here. Trying to pass the most minimal
    # data necessary to pbsmrtpipe.
    _d = dict(message=message, level=level, sourceId=source_id)
    warn_message = "Failed Request to {u} data: {d}".format(u=total_url, d=_d)

    def f():
        return _process_rpost(total_url, _d, headers=headers)

    return _run_func(f, warn_message, ignore_errors=ignore_errors)


def add_datastore_file(total_url, datastore_file, ignore_errors=True, headers=None):  # pragma: no cover
    """Add datastore to SMRT Server

    :type datastore_file: DataStoreFile
    """
    # Keeping this as public to avoid breaking pbsmrtpipe. The
    # new public interface should be the JobServiceClient
    _d = datastore_file.to_dict()
    warn_message = "Failed Request to {u} data: {d}.".format(u=total_url, d=_d)

    def f():
        return _process_rpost(total_url, _d, headers=headers)

    return _run_func(f, warn_message, ignore_errors=ignore_errors)


def _create_job_task(job_tasks_url, create_job_task_record, ignore_errors=True, headers=None):  # pragma: no cover
    """
    :type create_job_task_record: CreateJobTaskRecord
    :rtype: JobTask
    """
    warn_message = "Unable to create Task {c}".format(
        c=repr(create_job_task_record))

    def f():
        return _process_rpost_with_transform(JobTask.from_d)(
            job_tasks_url, create_job_task_record.to_dict(), headers=headers)

    return _run_func(f, warn_message, ignore_errors)


def _update_job_task_state(task_url, update_job_task_record, ignore_errors=True, headers=None):  # pragma: no cover
    """
    :type update_job_task_record: UpdateJobTaskRecord
    :rtype: JobTask
    """
    warn_message = "Unable to update Task {c}".format(
        c=repr(update_job_task_record))

    def f():
        return _process_rput_with_transform(JobTask.from_d)(
            task_url, update_job_task_record.to_dict(), headers=headers)

    return _run_func(f, warn_message, ignore_errors)


def _update_datastore_file(datastore_url, uuid, path, file_size, set_is_active,
                           ignore_errors=True, headers=None):  # pragma: no cover
    warn_message = "Unable to update datastore file {u}".format(u=uuid)
    total_url = "{b}/{u}".format(b=datastore_url, u=uuid)
    d = {"fileSize": file_size, "path": path, "isActive": set_is_active}

    def f():
        return _process_rput(total_url, d, headers=headers)

    return _run_func(f, warn_message, ignore_errors)


class CreateJobTaskRecord:

    def __init__(self, task_uuid, task_id, task_type_id,
                 name, state, created_at=None):
        self.task_uuid = task_uuid
        self.task_id = task_id
        self.task_type_id = task_type_id
        self.name = name
        # this must be consistent with the EngineJob states in the scala code
        self.state = state
        # Note, the created_at timestamp must have the form
        # 2016-02-18T23:24:46.569Z
        # or
        # 2016-02-18T15:24:46.569-08:00
        self.created_at = datetime.datetime.now(
            pytz.utc) if created_at is None else created_at

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  u=self.task_uuid,
                  i=self.task_id,
                  n=self.name,
                  s=self.state)
        return "<{k} uuid:{u} ix:{i} state:{s} name:{n} >".format(**_d)

    def to_dict(self):
        return dict(uuid=self.task_uuid,
                    taskId=self.task_id,
                    taskTypeId=self.task_type_id,
                    name=self.name,
                    state=self.state,
                    createdAt=self.created_at.isoformat())


class UpdateJobTaskRecord:

    def __init__(self, task_uuid, state, message, error_message=None):
        """:type error_message: str | None"""
        self.task_uuid = task_uuid
        self.state = state
        self.message = message
        # detailed error message (e.g., terse stack trace)
        self.error_message = error_message

    @staticmethod
    def from_error(task_uuid, state, message, error_message):
        # require an detailed error message
        return UpdateJobTaskRecord(task_uuid, state,
                                   message,
                                   error_message=error_message)

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.task_uuid,
                  s=self.state)
        return "<{k} i:{i} state:{s} >".format(**_d)

    def to_dict(self):
        _d = dict(uuid=self.task_uuid,
                  state=self.state,
                  message=self.message)

        # spray API is a little odd here. it will complain about
        # Expected String as JsString, but got null
        # even though the model is Option[String]
        if self.error_message is not None:
            _d['errorMessage'] = self.error_message

        return _d


class JobServiceClient:  # pragma: no cover
    # Keeping this class private. It should only be used from pbsmrtpipe

    def __init__(self, job_root_url, ignore_errors=False):
        """

        :param job_root_url: Full Root URL to the job
        :type job_root_url: str

        :param ignore_errors: Only log errors, don't not raise if a request fails. This is intended to be used in a fire-and-forget usecase
        :type ignore_errors: bool

        This hides the root location of the URL and hides
        the job id (as an int or uuid)

        The Url has the form:

        http://localhost:8888/smrt-link/job-manager/jobs/pbsmrtpipe/1234

        or

        http://localhost:8888/smrt-link/job-manager/jobs/pbsmrtpipe/5d562c74-e452-11e6-8b96-3c15c2cc8f88
        """
        self.job_root_url = job_root_url
        self.ignore_errors = ignore_errors

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, u=self.job_root_url)
        return "<{k} Job URL:{u} >".format(**_d)

    def _get_headers(self):
        return Constants.HEADERS

    def to_url(self, segment):
        return "{i}/{s}".format(i=self.job_root_url, s=segment)

    @property
    def log_url(self):
        return self.to_url("log")

    @property
    def datastore_url(self):
        return self.to_url("datastore")

    @property
    def tasks_url(self):
        return self.to_url("tasks")

    def get_task_url(self, task_uuid):
        """
        :param task_uuid: Task UUID
        :return:
        """
        return self.to_url("tasks/{t}".format(t=task_uuid))

    def log_workflow_progress(self, message, level,
                              source_id, ignore_errors=True):
        return log_pbsmrtpipe_progress(
            self.log_url, message, level, source_id, ignore_errors=ignore_errors)

    def add_datastore_file(self, datastore_file, ignore_errors=True):
        return add_datastore_file(
            self.datastore_url, datastore_file, ignore_errors=ignore_errors)

    def update_datastore_file(
            self, uuid, file_size=None, path=None, set_is_active=True, ignore_errors=True):
        return _update_datastore_file(
            self.datastore_url, uuid, path, file_size, set_is_active, ignore_errors)

    def create_task(self, task_uuid, task_id,
                    task_type_id, name, created_at=None):
        """

        :param task_uuid: Globally unique task id
        :param task_id: Unique within respect to the job
        :param task_type_id: ToolContract or task id (e.g., pbcommand.tasks.alpha)
        :param name: Display name of task
        :param created_at: time task was created at (will be set if current time if None)
        """
        r = CreateJobTaskRecord(task_uuid, task_id, task_type_id,
                                name, JobStates.CREATED, created_at=created_at)

        return _create_job_task(self.tasks_url, r)

    def update_task_status(self, task_uuid, state,
                           message, error_message=None):

        task_url = self.get_task_url(task_uuid)

        u = UpdateJobTaskRecord(
            task_uuid,
            state,
            message,
            error_message=error_message)

        return _update_job_task_state(
            task_url, u, ignore_errors=self.ignore_errors)

    def update_task_to_failed(self, task_uuid, message,
                              detailed_error_message):
        task_url = self.get_task_url(task_uuid)
        state = JobStates.FAILED

        u = UpdateJobTaskRecord(task_uuid,
                                state,
                                message,
                                error_message=detailed_error_message)

        return _update_job_task_state(task_url, u)


# -----------------------------------------------------------------------
# SSL stuff
class Wso2Constants:  # pragma: no cover
    SECRET = "KMLz5g7fbmx8RVFKKdu0NOrJic4a"
    CONSUMER_KEY = "6NjRXBcFfLZOwHc0Xlidiz4ywcsa"
    SCOPES = ["welcome", "run-design", "run-qc", "openid", "analysis",
              "sample-setup", "data-management", "userinfo"]


def _create_auth(secret, consumer_key):  # pragma: no cover
    return base64.b64encode(":".join([secret, consumer_key]).encode("utf-8"))


def get_token(url, user, password, scopes, secret, consumer_key):  # pragma: no cover
    basic_auth = _create_auth(secret, consumer_key).decode("utf-8")
    # To be explicit for pedagogical purposes
    headers = {
        "Authorization": "Basic {}".format(basic_auth),
        "Content-Type": "application/x-www-form-urlencoded"
    }

    scope_str = " ".join({s for s in scopes})
    payload = dict(grant_type="password",
                   username=user,
                   password=password,
                   scope=scope_str)

    # verify is false to disable the SSL cert verification
    return requests.post(url, payload, headers=headers, verify=False)


def _get_smrtlink_wso2_token(user, password, url):  # pragma: no cover
    r = get_token(
        url,
        user,
        password,
        Wso2Constants.SCOPES,
        Wso2Constants.SECRET,
        Wso2Constants.CONSUMER_KEY)
    j = r.json()
    access_token = j['access_token']
    refresh_token = j['refresh_token']
    scopes = j['scope'].split(" ")
    return access_token, refresh_token, scopes


class SmrtLinkAuthClient(ServiceAccessLayer):  # pragma: no cover
    """
    HTTPS-enabled client that routes via WSO2 and requires authentication.
    For internal use only - this is NOT an officially supported API.  Currently
    somewhat sloppy w.r.t. SSL security features.
    """

    def __init__(self, base_url, user, password, port=8243, debug=False,
                 sleep_time=2, token=None):
        super().__init__(
            base_url,
            port,
            debug=debug,
            sleep_time=sleep_time)
        self._user = user
        self._password = password

        if token is None:
            if (user is None or password is None):
                raise ValueError(
                    "Both user and password must be defined unless an existing auth token is supplied")
            self._login()
        else:
            # assume token is valid. This will fail on the first client request
            # if not valid with an obvious error message
            self.auth_token = token
            self.refresh_token = None

    def _login(self):
        url = "{u}:{p}/token".format(u=self.base_url, p=self.port)
        self.auth_token, self.refresh_token, _ = _get_smrtlink_wso2_token(
            self._user, self._password, url)

    def _get_headers(self):
        return {
            "Authorization": "Bearer {}".format(self.auth_token),
            "Content-type": "application/json"
        }

    def _to_base_url(self, h):
        if h.startswith("http://"):
            raise ValueError("Invalid URL - this client requires HTTPS")
        prefix = "https://"
        return h if h.startswith(prefix) else prefix + h

    @property
    def uri(self):
        return "{b}:{u}/SMRTLink/1.0.0".format(b=self.base_url, u=self.port)

    def reauthenticate_if_necessary(self):
        """
        Check whether the client still has authorization to access the /status
        endpoint, and acquire a new auth token if not.
        """
        try:
            status = self.get_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                self._login()
            else:
                raise e


def get_smrtlink_client(host, port, user=None, password=None, sleep_time=5):  # pragma: no cover
    """
    Convenience method for use in CLI testing tools.  Returns an instance of
    the appropriate client class given the input parameters.  Unlike the client
    itself this hardcodes 8243 as the WSO2 port number.
    """
    if host != "localhost" or None not in [user, password]:
        return SmrtLinkAuthClient(host, user, password, sleep_time=sleep_time)
    else:
        return ServiceAccessLayer(host, port, sleep_time=sleep_time)
