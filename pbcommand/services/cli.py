"""CLI for interacting with the PacBio Services

0.1.0 Version, Import/Convert datasets

pbservice import-dataset # dir or XML file
pbservice import-rs-movie # dir or XML file (Requires 'movie-to-dataset' exe)
pbservice import-ref-info # dir or XML file (Requires 'reference-to-dataset' exe)
pbservice import-fasta /path/to/file.fasta --name my-name --organism my-org --ploidy haploid

0.2.0 Version, Jobs Support, leveraging

pbservice run-analysis path/to/file.json
pbservice run-merge-dataset path/to/file.json


"""
import argparse
import json

import os
import pprint
import sys
import logging
import functools
import time
import traceback
import uuid

from requests import RequestException
import iso8601

from pbcommand.cli import get_default_argparser_with_base_opts
from pbcommand.models import FileTypes
from pbcommand.services import (ServiceAccessLayer,
                                ServiceEntryPoint,
                                JobExeError)
from pbcommand.services.service_access_layer import (DATASET_METATYPES_TO_ENDPOINTS, )
from pbcommand.validators import validate_file, validate_or
from pbcommand.common_options import add_common_options
from pbcommand.utils import (is_dataset,
                             walker, setup_log, compose, setup_logger,
                             get_parsed_args_log_level)

from .utils import to_ascii

__version__ = "0.2.0"

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())  # suppress warning message


_LOG_FORMAT = '[%(levelname)s] %(asctime)-15s %(message)s'


def _list_dict_printer(list_d):
    for i in list_d:
        print i

try:
    # keep this to keep backward compatible
    from tabulate import tabulate

    def printer(list_d):
        print tabulate(list_d)
    list_dict_printer = printer
except ImportError:
    list_dict_printer = _list_dict_printer


class Constants(object):

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


def validate_xml_file_or_dir(path):
    px = os.path.abspath(os.path.expanduser(path))
    if os.path.isdir(px):
        return px
    elif os.path.isfile(px) and _is_xml(px):
        return px
    else:
        raise argparse.ArgumentTypeError("Expected dir or file '{p}'".format(p=path))


validate_int_or_uuid = validate_or(int, uuid.UUID, "Expected Int or UUID")


def _get_size_mb(path):
    return os.stat(path).st_size / 1024.0 / 1024.0


def validate_file_and_size(max_size_mb):
    def _wrapper(path):
        p = validate_file(path)
        sx = _get_size_mb(path)
        if sx > max_size_mb:
            raise argparse.ArgumentTypeError("Fasta file is too large {s:.2f} MB > {m:.2f} MB. Create a ReferenceSet using {e}, then import using `pbservice import-dataset /path/to/referenceset.xml` ".format(e=Constants.FASTA_TO_REFERENCE, s=sx, m=Constants.MAX_FASTA_FILE_MB))
        else:
            return p
    return _wrapper


validate_max_fasta_file_size = validate_file_and_size(Constants.MAX_FASTA_FILE_MB)


def add_block_option(p):
    p.add_argument('--block', action='store_true', default=False,
                   help="Block during importing process")
    return p


def add_sal_options(p):

    default_port = os.environ.get(Constants.ENV_PB_SERVICE_PORT, Constants.DEFAULT_PORT)
    default_host = os.environ.get(Constants.ENV_PB_SERVICE_HOST, Constants.DEFAULT_HOST)

    p.add_argument('--host', type=str,
                   default=default_host,
                   help="Server host. Override the default with env {v}".format(v=Constants.ENV_PB_SERVICE_HOST))
    p.add_argument('--port', type=int, default=default_port,
                   help="Server Port. Override default with env {v}".format(v=Constants.ENV_PB_SERVICE_PORT))
    return p


def add_base_and_sal_options(p):
    fx = [add_common_options, add_sal_options]
    f = compose(*fx)
    return f(p)


def add_xml_or_dir_option(p):
    p.add_argument('xml_or_dir', type=validate_xml_file_or_dir, help="Directory or XML file.")
    return p


def add_sal_and_xml_dir_options(p):
    fx = [add_common_options,
          add_sal_options,
          add_xml_or_dir_option]
    f = compose(*fx)
    return f(p)


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
                        log.exception("Import failed because the underlying "+
                                      "data appear to be corrupted.  Run "+
                                      "'pbvalidate' on the dataset for more "+
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


def args_runner_import_datasets(args):
    return run_import_local_datasets(args.host, args.port, args.xml_or_dir)


def add_import_fasta_opts(p):
    px = p.add_argument
    px('fasta_path', type=validate_max_fasta_file_size, help="Path to Fasta File")
    px('--name', required=True, type=str, help="Name of ReferenceSet")
    px('--organism', required=True, type=str, help="Organism")
    px('--ploidy', required=True, type=str, help="Ploidy")
    add_block_option(p)
    add_sal_options(p)
    add_common_options(p)
    return p


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


def args_run_import_fasta(args):
    log.debug(args)
    return run_import_fasta(args.host, args.port, args.fasta_path,
                            args.name, args.organism, args.ploidy, block=args.block)


def load_analysis_job_json(d):
    """Translate a dict to args for scenario runner inputs"""
    job_name = to_ascii(d['name'])
    pipeline_template_id = to_ascii(d["pipelineId"])
    service_epoints = [ServiceEntryPoint.from_d(x) for x in d['entryPoints']]
    return job_name, pipeline_template_id, service_epoints


def _validate_analysis_job_json(path):
    px = validate_file(path)
    with open(px, 'r') as f:
        d = json.loads(f.read())

    try:
        load_analysis_job_json(d)
        return px
    except (KeyError, TypeError, ValueError) as e:
        raise argparse.ArgumentTypeError("Invalid analysis.json format for '{p}' {e}".format(p=px, e=repr(e)))


def add_run_analysis_job_opts(p):
    p.add_argument('json_path', type=_validate_analysis_job_json, help="Path to analysis.json file")
    add_sal_options(p)
    add_common_options(p)
    add_block_option(p)
    return


def run_analysis_job(sal, job_name, pipeline_id, service_entry_points, block=False, time_out=None, task_options=()):
    """Run analysis (pbsmrtpipe) job

    :rtype ServiceJob:
    """
    if time_out is None:
        time_out = sal.JOB_DEFAULT_TIMEOUT
    status = sal.get_status()
    log.info("Status {x}".format(x=status['message']))

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
        job_result = sal.run_by_pipeline_template_id(job_name, pipeline_id, resolved_service_entry_points, time_out=time_out, task_options=task_options)
        job_id = job_result.job.id
        # service job
        result = sal.get_analysis_job_by_id(job_id)
        if not result.was_successful():
            raise JobExeError("Job {i} failed".format(i=job_id))
    else:
        # service job or error
        result = sal.create_by_pipeline_template_id(job_name, pipeline_id, resolved_service_entry_points)

    log.info("Result {r}".format(r=result))
    return result


def args_run_analysis_job(args):
    log.debug(args)
    with open(args.json_path, 'r') as f:
        d = json.loads(f.read())

    log.debug("Loaded \n" + pprint.pformat(d))
    job_name, pipeline_id, service_entry_points = load_analysis_job_json(d)

    sal = ServiceAccessLayer(args.host, args.port)
    # this should raise if there's a failure
    result = run_analysis_job(sal, job_name, pipeline_id, service_entry_points, block=args.block)
    return 0


def args_emit_analysis_template(args):
    ep1 = ServiceEntryPoint("eid_ref_dataset", FileTypes.DS_REF.file_type_id, 1)
    ep1_d = ep1.to_d()
    ep1_d['_comment'] = "datasetId can be provided as the DataSet UUID or Int. The entryId(s) can be obtained by running 'pbsmrtpipe show-pipeline-templates {PIPELINE-ID}'"
    d = dict(name="Job name",
             pipelineId="pbsmrtpipe.pipelines.dev_diagnostic",
             entryPoints=[ep1_d],
             taskOptions=[],
             workflowOptions=[])

    sx = json.dumps(d, sort_keys=True, indent=4, separators=(',', ': '))
    print sx

    return 0


def args_get_sal_summary(args):

    host = args.host
    port = args.port

    sal = ServiceAccessLayer(host, port)

    print sal.to_summary()

    return 0


def add_get_job_options(p):
    add_base_and_sal_options(p)
    p.add_argument("job_id", type=validate_int_or_uuid, help="Job id or UUID")
    return p


def run_get_job_summary(host, port, job_id):
    sal = get_sal_and_status(host, port)
    job = sal.get_job_by_id(job_id)

    if job is None:
        log.error("Unable to find job {i} from {u}".format(i=job_id, u=sal.uri))
    else:
        print job

    return 0


def add_get_job_list_options(p):
    fs = [add_base_and_sal_options,
          add_max_items_option(25, "Max Number of jobs")]
    f = compose(*fs)
    return f(p)


def args_get_job_summary(args):
    return run_get_job_summary(args.host, args.port, args.job_id)


def run_job_list_summary(host, port, max_items, sort_by=None):
    sal = get_sal_and_status(host, port)

    jobs = sal.get_analysis_jobs()

    jobs_list = jobs if sort_by is None else sorted(jobs, cmp=sort_by)

    printer(jobs_list[:max_items])

    return 0


def args_get_job_list_summary(args):
    return run_job_list_summary(args.host, args.port, args.max_items, sort_by=_cmp_sort_by_id_desc)


def add_get_dataset_options(p):
    add_base_and_sal_options(p)
    p.add_argument('id_or_uuid', type=validate_int_or_uuid, help="DataSet Id or UUID")
    return p


def add_get_dataset_list_options(p):
    add_base_and_sal_options(p)
    fx = add_max_items_option(25, "Max number of Datasets to show")
    fx(p)
    default_dataset_type = DATASET_METATYPES_TO_ENDPOINTS[FileTypes.DS_SUBREADS]
    # this should be choice
    p.add_argument('-t', '--dataset-type', type=str, default=default_dataset_type, help="DataSet Meta type")
    return p


def run_get_dataset_summary(host, port, dataset_id_or_uuid):

    sal = get_sal_and_status(host, port)

    log.debug("Getting dataset {d}".format(d=dataset_id_or_uuid))
    ds = sal.get_dataset_by_uuid(dataset_id_or_uuid)

    if ds is None:
        log.error("Unable to find DataSet '{i}' on {u}".format(i=dataset_id_or_uuid, u=sal.uri))
    else:
        print pprint.pformat(ds, indent=2)

    return 0


def _cmp_sort_by_id_key_desc(a, b):
    return b['id'] - a['id']


def _cmp_sort_by_id_desc(a, b):
    return b.id - a.id


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
        raise KeyError("Unsupported dataset type {t} Supported types {s}".format(t=dataset_type, s=fs.keys()))
    else:
        datasets = f()
        # this needs to be improved
        sorted_datasets = datasets if sort_by is None else sorted(datasets, cmp=sort_by)

        print "Number of {t} Datasets {n}".format(t=dataset_type, n=len(datasets))
        list_dict_printer(sorted_datasets[:max_items])

    return 0


def args_run_dataset_summary(args):
    return run_get_dataset_summary(args.host, args.port, args.id_or_uuid)


def args_run_dataset_list_summary(args):
    return run_get_dataset_list_summary(args.host,
                                        args.port,
                                        args.dataset_type,
                                        args.max_items,
                                        sort_by=_cmp_sort_by_id_key_desc)


def subparser_builder(subparser, subparser_id, description, options_func, exe_func):
    """
    Util to add subparser options

    :param subparser:
    :param subparser_id:
    :param description:
    :param options_func: Function that will add args and options to Parser instance F(subparser) -> None
    :param exe_func: Function to run F(args) -> Int
    :return:
    """
    p = subparser.add_parser(subparser_id, help=description,
                             formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    options_func(p)
    p.set_defaults(func=exe_func)
    return p


def get_parser():
    desc = "Tool to import datasets, convert/import fasta file and run analysis jobs"
    p = get_default_argparser_with_base_opts(__version__, desc)

    sp = p.add_subparsers(help='commands')

    def builder(subparser_id, description, options_func, exe_func):
        subparser_builder(sp, subparser_id, description, options_func, exe_func)

    status_desc = "Get System Status, DataSet and Job Summary"
    builder('status', status_desc, add_base_and_sal_options, args_get_sal_summary)

    local_desc = " The file location must be accessible from the host where the Services are running (often on a shared file system)"
    ds_desc = "Import Local DataSet XML." + local_desc
    builder('import-dataset', ds_desc, add_sal_and_xml_dir_options, args_runner_import_datasets)

    fasta_desc = "Import Fasta (and convert to ReferenceSet)." + local_desc
    builder("import-fasta", fasta_desc, add_import_fasta_opts, args_run_import_fasta)

    run_analysis_desc = "Run Secondary Analysis Pipeline using an analysis.json"
    builder("run-analysis", run_analysis_desc, add_run_analysis_job_opts, args_run_analysis_job)

    emit_analysis_json_desc = "Emit an analysis.json Template to stdout that can be run using 'run-analysis'"
    builder("emit-analysis-template", emit_analysis_json_desc, add_common_options, args_emit_analysis_template)

    # Get Summary Job by Id
    job_summary_desc = "Get Job Summary by Job Id"
    builder('get-job', job_summary_desc, add_get_job_options, args_get_job_summary)

    job_list_summary_desc = "Get Job Summary by Job Id"
    builder('get-jobs', job_list_summary_desc, add_get_job_list_options, args_get_job_list_summary)

    ds_summary_desc = "Get DataSet Summary by DataSet Id or UUID"
    builder('get-dataset', ds_summary_desc, add_get_dataset_options, args_run_dataset_summary)

    ds_list_summary_desc = "Get DataSet List Summary by DataSet Type"
    builder('get-datasets', ds_list_summary_desc, add_get_dataset_list_options, args_run_dataset_list_summary)

    return p


def args_executer(args):
    """
    This is pulled from pbsmrtpipe that uses the goofy func=my_runner_func,
    which will be called using args.func(args)

    :rtype int
    """
    try:

        return_code = args.func(args)
    except Exception as e:
        if isinstance(e, RequestException):
            # make this terse so there's not a useless stacktrace
            emsg = "Failed to connect to SmrtServer {e}".format(e=repr(e.__class__.__name__))
            log.error(emsg)
            return_code = 3
        elif isinstance(e, IOError):
            log.error(e, exc_info=True)
            traceback.print_exc(sys.stderr)
            return_code = 1
        else:
            log.error(e, exc_info=True)
            traceback.print_exc(sys.stderr)
            return_code = 2

    return return_code


def main_runner(argv, parser, exe_runner_func,
                level=logging.DEBUG, str_formatter=_LOG_FORMAT):
    """
    Fundamental interface to commandline applications
    """
    started_at = time.time()
    args = parser.parse_args(argv)

    level = get_parsed_args_log_level(args, default_level=level)
    console_or_file = args.log_file
    setup_logger(console_or_file, level, formatter=str_formatter)

    log.debug(args)
    log.info("Starting tool version {v}".format(v=parser.version))

    rcode = exe_runner_func(args)

    run_time = time.time() - started_at
    _d = dict(r=rcode, s=run_time)
    log.info("exiting with return code {r} in {s:.2f} sec.".format(**_d))
    return rcode


def main(argv=None):

    argv_ = sys.argv if argv is None else argv
    parser = get_parser()

    return main_runner(argv_[1:], parser, args_executer)
