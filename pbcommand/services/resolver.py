#!/usr/bin/env python

"""
Utility to obtain paths to important analysis files from SMRT Link jobs,
compatible with multiple applications and versions.
"""

import logging
import os.path as op
import os
import sys

from pbcommand.models.common import FileTypes
from pbcommand.services._service_access_layer import get_smrtlink_client
from pbcommand.utils import setup_log
from pbcommand.cli import get_default_argparser_with_base_opts, pacbio_args_runner

log = logging.getLogger(__name__)
__version__ = "0.1.1"


class ResolverFailure(Exception):
    pass


class ResourceTypes:
    JOB_PATH = "path"
    ALIGNMENTS = "alignments"
    PREASSEMBLY = "preassembly"
    POLISHED_ASSEMBLY = "polished-assembly"
    MAPPING_STATS = "mapping-stats"
    SUBREADS_ENTRY = "subreads-in"

    ALL = [JOB_PATH, ALIGNMENTS, PREASSEMBLY,
           POLISHED_ASSEMBLY, MAPPING_STATS, SUBREADS_ENTRY]

    @staticmethod
    def from_string(s):
        if s in ResourceTypes.ALL:
            return s
        else:
            raise KeyError("Unknown resource type '%s'" % s)


def _is_report(ds_file):
    return ds_file.file_type_id == FileTypes.REPORT.file_type_id


def _is_alignments(ds_file):
    return ds_file.file_type_id in [FileTypes.DS_ALIGN.file_type_id,
                                    FileTypes.DS_ALIGN_CCS.file_type_id]


def _get_by_partial_source_id(ds_files, source_id_str):
    for ds_file in ds_files:
        if source_id_str in ds_file.source_id:
            return ds_file.path


ALIGNMENT_SOURCES = [
    "mapped",  # new (Cromwell) mapping and resequencing workflows
    "consolidated_xml",  # new resequencing workflow
    "consolidate_alignments-out-0",  # old (pbsmrtpipe) resequencing pipeline
    "consolidate_alignments_ccs-out-0",  # old CCS mapping pipelines
    "datastore_to_alignments-out-0",  # old mapping/resequencing pipelines
    "datastore_to_ccs_alignments-out-0"  # old CCS mapping pipelines
]


def _find_alignments(datastore):
    alignments = [f for f in datastore.files.values() if _is_alignments(f)]
    if len(alignments) == 1:
        return alignments[0].path
    for source in ALIGNMENT_SOURCES:
        for ds_file in alignments:
            source_id = ds_file.source_id.split(".")[-1]
            if source_id == source:
                return ds_file.path
    raise ResolverFailure("Can't find alignments output for job")


class Resolver:
    def __init__(self,
                 host,
                 port,
                 user=None,
                 password=None):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._client = get_smrtlink_client(host, port, user, password)

    def _get_job_datastore_reports(self, job_id):
        datastore = self._client.get_analysis_job_datastore(job_id)
        return [f for f in datastore.files.values() if _is_report(f)]

    def resolve_alignments(self, job_id):
        datastore = self._client.get_analysis_job_datastore(job_id)
        return _find_alignments(datastore)

    def resolve_preassembly_stats(self, job_id):
        ds_files = self._get_job_datastore_reports(job_id)
        return _get_by_partial_source_id(ds_files, "preassembly")

    def resolve_polished_assembly_stats(self, job_id):
        ds_files = self._get_job_datastore_reports(job_id)
        return _get_by_partial_source_id(ds_files, "polished_assembly")

    def resolve_mapping_stats(self, job_id):
        ds_files = self._get_job_datastore_reports(job_id)
        return _get_by_partial_source_id(ds_files, "mapping_stats")

    def resolve_job(self, job_id):
        return self._client.get_job_by_id(job_id).path

    def resolve_input_subreads(self, job_id):
        eps = self._client.get_analysis_job_entry_points(job_id)
        subread_ids = []
        for ep in eps:
            if ep.dataset_metatype == FileTypes.DS_SUBREADS.file_type_id:
                subread_ids.append(ep.dataset_uuid)
        if len(subread_ids) == 0:
            raise ResolverFailure(
                "Can't find a SubreadSet entry point for this job")
        elif len(subread_ids) > 1:
            raise ResolverFailure(
                "Multiple SubreadSet entry points found for this job")
        return self._client.get_subreadset_by_id(subread_ids[0])["path"]


def run_args(args):
    resolver = Resolver(args.host, args.port, args.user, args.password)
    resource = None
    if args.resource_type == ResourceTypes.JOB_PATH:
        resource = resolver.resolve_job(args.job_id)
    elif args.resource_type == ResourceTypes.ALIGNMENTS:
        resource = resolver.resolve_alignments(args.job_id)
    elif args.resource_type == ResourceTypes.PREASSEMBLY:
        resource = resolver.resolve_preassembly_stats(args.job_id)
    elif args.resource_type == ResourceTypes.POLISHED_ASSEMBLY:
        resource = resolver.resolve_polished_assembly_stats(args.job_id)
    elif args.resource_type == ResourceTypes.MAPPING_STATS:
        resource = resolver.resolve_mapping_stats(args.job_id)
    elif args.resource_type == ResourceTypes.SUBREADS_ENTRY:
        resource = resolver.resolve_input_subreads(args.job_id)
    else:
        raise NotImplementedError("Can't retrieve resource type '%s'" % args.resource_type)
    print(resource)
    if args.make_symlink is not None:
        if op.exists(args.make_symlink):
            os.remove(args.make_symlink)
        os.symlink(resource, args.make_symlink)
    return 0


def _get_parser():
    p = get_default_argparser_with_base_opts(
        __version__,
        __doc__,
        default_level=logging.WARN)
    p.add_argument("job_id", help="SMRT Link analysis job ID")
    p.add_argument("resource_type", nargs="?",
                   default=ResourceTypes.JOB_PATH,
                   type=ResourceTypes.from_string,
                   help="Resource type to resolve (choices: {c})".format(
                        c=", ".join(ResourceTypes.ALL)))
    p.add_argument("-u", "--host", dest="host", action="store",
                   default=os.environ.get("PB_SERVICE_HOST", "localhost"),
                   help="Hostname of SMRT Link server.  If this is anything other than 'localhost' you must supply authentication.")
    p.add_argument("-p", "--port", dest="port", action="store", type=int,
                   default=int(os.environ.get("PB_SERVICE_PORT", "8081")),
                   help="Services port number")
    p.add_argument("--user", dest="user", action="store",
                   default=os.environ.get("PB_SERVICE_AUTH_USER", None),
                   help="User to authenticate with (if using HTTPS)")
    p.add_argument("--password", dest="password", action="store",
                   default=os.environ.get("PB_SERVICE_AUTH_PASSWORD", None),
                   help="Password to authenticate with (if using HTTPS)")
    p.add_argument("--symlink", dest="make_symlink", action="store",
                   default=None,
                   help="If defined, will create a symlink to the retrieved file")
    return p


def main(argv):
    return pacbio_args_runner(argv[1:],
                              _get_parser(),
                              run_args,
                              log,
                              setup_log_func=setup_log)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
