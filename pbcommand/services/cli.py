"""
Utilities for streamlining common services actions (used in pbcromwell test
runner among others).
The old CLI program is largely replaced by the Scala version in 'smrtflow'.
"""

from functools import cmp_to_key
import functools
import logging
import pprint
import os
import sys
import json
import time
import uuid

from requests import RequestException
import iso8601

from pbcommand.models import FileTypes
from pbcommand.services import (ServiceAccessLayer,
                                ServiceEntryPoint,
                                JobExeError)
from pbcommand.services._service_access_layer import (DATASET_METATYPES_TO_ENDPOINTS, )
from pbcommand.validators import validate_file, validate_or
from pbcommand.utils import (is_dataset, walker, compose)
from pbcommand import to_ascii

__version__ = "0.3.0"

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())  # suppress warning message


def _list_dict_printer(list_d):
    for i in list_d:
        print(i)


try:
    # keep this to keep backward compatible
    from tabulate import tabulate

    def printer(list_d):
        print(tabulate(list_d))
    list_dict_printer = printer
except ImportError:
    list_dict_printer = _list_dict_printer


class Constants:

    # When running from the commandline, the host and port will default to these
    # values if provided
    ENV_PB_SERVICE_HOST = "PB_SERVICE_HOST"
    ENV_PB_SERVICE_PORT = "PB_SERVICE_PORT"

    DEFAULT_HOST = "http://localhost"
    DEFAULT_PORT = 8070

    FASTA_TO_REFERENCE = "fasta-to-reference"
    RS_MOVIE_TO_DS = "movie-metadata-to-dataset"

    # Currently only small-ish files are supported, users should
    # use fasta-to-reference offline and import the reference set
    MAX_FASTA_FILE_MB = 100


def _is_xml(path):
    return path.endswith(".xml")


def add_max_items_option(default, desc="Max items to return"):
    def f(p):
        p.add_argument('-m', '--max-items', type=int, default=default, help=desc)
        return p
    return f


validate_int_or_uuid = validate_or(int, uuid.UUID, "Expected Int or UUID")


def _get_size_mb(path):
    return os.stat(path).st_size / 1024.0 / 1024.0


def get_sal_and_status(host, port):
    """Get Sal or Raise if status isn't successful"""
    try:
        sal = ServiceAccessLayer(host, port)
        sal.get_status()
        return sal
    except RequestException as e:
        log.error("Failed to connect to {h}:{p}".format(h=host, p=port))
        raise


def run_file_or_dir(file_func, dir_func, xml_or_dir):
    if os.path.isdir(xml_or_dir):
        return dir_func(xml_or_dir)
    elif os.path.isfile(xml_or_dir):
        return file_func(xml_or_dir)
    else:
        raise ValueError("Unsupported value {x}".format(x=xml_or_dir))


def is_xml_dataset(path):
    if _is_xml(path):
        if is_dataset(path):
            return True
    return False


def dataset_walker(root_dir):
    filter_func = is_xml_dataset
    return walker(root_dir, filter_func)


def import_local_dataset(sal, path):
    """:type sal: ServiceAccessLayer"""
    # XXX basic validation of external resources
    try:
        from pbcore.io import openDataSet, ReadSet, HdfSubreadSet
    except ImportError:
        log.warn("Can't import pbcore, skipping dataset sanity check")
    else:
        ds = openDataSet(path, strict=True)
        if isinstance(ds, ReadSet) and not isinstance(ds, HdfSubreadSet):
            if len(ds) > 0:
                log.info("checking BAM file integrity")
                for rr in ds.resourceReaders():
                    try:
                        _ = rr[-1]
                    except Exception as e:
                        log.exception("Import failed because the underlying " +
                                      "data appear to be corrupted.  Run " +
                                      "'pbvalidate' on the dataset for more " +
                                      "thorough checking.")
                        return 1
            else:
                log.warn("Empty dataset - will import anyway")

    # this will raise if the import wasn't successful
    _ = sal.run_import_local_dataset(path)
    log.info("Successfully import dataset from {f}".format(f=path))
    return 0


def import_datasets(sal, root_dir):
    # FIXME. Need to add a flag to keep importing even if an import fails
    rcodes = []
    for path in dataset_walker(root_dir):
        try:
            import_local_dataset(sal, path)
            rcodes.append(0)
        except Exception as e:
            log.error("Failed to import dataset {e}".format(e=e))
            rcodes.append(1)

    state = all(v == 0 for v in rcodes)
    return 0 if state else 1


def run_import_local_datasets(host, port, xml_or_dir):
    sal = ServiceAccessLayer(host, port)
    file_func = functools.partial(import_local_dataset, sal)
    dir_func = functools.partial(import_datasets, sal)
    return run_file_or_dir(file_func, dir_func, xml_or_dir)


def run_import_fasta(host, port, fasta_path, name, organism, ploidy, block=False):
    sal = ServiceAccessLayer(host, port)
    log.info("importing ({s:.2f} MB) {f} ".format(s=_get_size_mb(fasta_path), f=fasta_path))
    if block is True:
        result = sal.run_import_fasta(fasta_path, name, organism, ploidy)
        log.info("Successfully imported {f}".format(f=fasta_path))
        log.info("result {r}".format(r=result))
    else:
        sal.import_fasta(fasta_path, name, organism, ploidy)

    return 0


def load_analysis_job_json(d):
    """Translate a dict to args for scenario runner inputs"""
    job_name = to_ascii(d['name'])
    pipeline_template_id = to_ascii(d["pipelineId"])
    service_epoints = [ServiceEntryPoint.from_d(x) for x in d['entryPoints']]
    tags = d.get('tags', [])
    return job_name, pipeline_template_id, service_epoints, tags


def run_analysis_job(sal, job_name, pipeline_id, service_entry_points, block=False, time_out=None, task_options=(), tags=()):
    """Run analysis (pbsmrtpipe) job

    :rtype ServiceJob:
    """
    if time_out is None:
        time_out = sal.JOB_DEFAULT_TIMEOUT
    status = sal.get_status()
    log.info("System:{i} v:{v} Status:{x}".format(x=status['message'], i=status['id'], v=status['version']))

    resolved_service_entry_points = []
    for service_entry_point in service_entry_points:
        # Always lookup/resolve the dataset by looking up the id
        ds = sal.get_dataset_by_uuid(service_entry_point.resource)
        if ds is None:
            raise ValueError("Failed to find DataSet with id {r} {s}".format(s=service_entry_point, r=service_entry_point.resource))

        dataset_id = ds['id']
        ep = ServiceEntryPoint(service_entry_point.entry_id, service_entry_point.dataset_type, dataset_id)
        log.debug("Resolved dataset {e}".format(e=ep))
        resolved_service_entry_points.append(ep)

    if block:
        job_result = sal.run_by_pipeline_template_id(job_name, pipeline_id, resolved_service_entry_points, time_out=time_out, task_options=task_options, tags=tags)
        job_id = job_result.job.id
        # service job
        result = sal.get_analysis_job_by_id(job_id)
        if not result.was_successful():
            raise JobExeError("Job {i} failed:\n{e}".format(i=job_id, e=job_result.job.error_message))
    else:
        # service job or error
        result = sal.create_by_pipeline_template_id(job_name, pipeline_id, resolved_service_entry_points, tags=tags)

    log.info("Result {r}".format(r=result))
    return result


def run_get_job_summary(host, port, job_id):
    sal = get_sal_and_status(host, port)
    job = sal.get_job_by_id(job_id)
    epoints = sal.get_analysis_job_entry_points(job_id)

    if job is None:
        log.error("Unable to find job {i} from {u}".format(i=job_id, u=sal.uri))
    else:
        # this is not awesome, but the scala code should be the fundamental
        # tool
        print("Job {}".format(job_id))
        # The settings will often make this unreadable
        print(job._replace(settings={}))
        print(" Entry Points {}".format(len(epoints)))
        for epoint in epoints:
            print("  {}".format(epoint))

    return 0


def run_job_list_summary(host, port, max_items, sort_by=None):
    sal = get_sal_and_status(host, port)

    jobs = sal.get_analysis_jobs()

    jobs_list = jobs if sort_by is None else sorted(jobs, key=cmp_to_key(sort_by))

    printer(jobs_list[:max_items])

    return 0


def run_get_dataset_summary(host, port, dataset_id_or_uuid):

    sal = get_sal_and_status(host, port)

    log.debug("Getting dataset {d}".format(d=dataset_id_or_uuid))
    ds = sal.get_dataset_by_uuid(dataset_id_or_uuid)

    if ds is None:
        log.error("Unable to find DataSet '{i}' on {u}".format(i=dataset_id_or_uuid, u=sal.uri))
    else:
        print(pprint.pformat(ds, indent=2))

    return 0


def run_get_dataset_list_summary(host, port, dataset_type, max_items, sort_by=None):
    """

    Display a list of Dataset summaries

    :param host:
    :param port:
    :param dataset_type:
    :param max_items:
    :param sort_by: func to sort resources sort_by = lambda x.created_at
    :return:
    """
    sal = get_sal_and_status(host, port)

    def to_ep(file_type):
        return DATASET_METATYPES_TO_ENDPOINTS[file_type]

    # FIXME(mkocher)(2016-3-26) need to centralize this on the dataset "shortname"?
    fs = {to_ep(FileTypes.DS_SUBREADS): sal.get_subreadsets,
          to_ep(FileTypes.DS_REF): sal.get_referencesets,
          to_ep(FileTypes.DS_ALIGN): sal.get_alignmentsets,
          to_ep(FileTypes.DS_BARCODE): sal.get_barcodesets
          }

    f = fs.get(dataset_type)

    if f is None:
        raise KeyError("Unsupported dataset type {t} Supported types {s}".format(t=dataset_type, s=list(fs.keys())))
    else:
        datasets = f()
        # this needs to be improved
        sorted_datasets = datasets if sort_by is None else sorted(datasets, key=cmp_to_key(sort_by))

        print("Number of {t} Datasets {n}".format(t=dataset_type, n=len(datasets)))
        list_dict_printer(sorted_datasets[:max_items])

    return 0
