"""Core models used in the ToolContract and Resolved ToolContract


Large parts of this are pulled from pbsmrtpipe.

Author: Michael Kocher
"""
import logging
import warnings
import functools

log = logging.getLogger(__name__)

REGISTERED_FILE_TYPES = {}


class PacBioNamespaces(object):
    # File Types
    #PBSMRTPIPE_FILE_PREFIX = 'pbsmrtpipe.files'
    # NEW File Type Identifier style Prefix
    NEW_PBSMRTPIPE_FILE_PREFIX = "PacBio.FileTypes"
    # New DataSet Identifier Prefix
    DATASET_FILE_PREFIX = "PacBio.DataSet"
    # Task Ids
    PBSMRTPIPE_TASK_PREFIX = 'pbsmrtpipe.tasks'
    # Task Options
    PBSMRTPIPE_TASK_OPTS_PREFIX = 'pbsmrtpipe.task_options'
    # Workflow Level Options
    PBSMRTPIPE_OPTS_PREFIX = 'pbsmrtpipe.options'
    # Constants
    PBSMRTPIPE_CONSTANTS_PREFIX = 'pbsmrtpipe.constants'
    # Pipelines
    PBSMRTPIPE_PIPELINES = "pbsmrtpipe.pipelines"


def __to_type(prefix, name):
    return ".".join([prefix, name])

to_constant_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_CONSTANTS_PREFIX)
to_file_ns = functools.partial(__to_type, PacBioNamespaces.NEW_PBSMRTPIPE_FILE_PREFIX)
to_ds_ns = functools.partial(__to_type, PacBioNamespaces.DATASET_FILE_PREFIX)
to_task_option_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_TASK_OPTS_PREFIX)
to_task_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_TASK_PREFIX)
to_workflow_option_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_OPTS_PREFIX)
to_pipeline_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_PIPELINES)


class TaskTypes(object):
    """Task types used in workflow engine. Local will run the task as a subprocess,
    Distributed will run the process on a remote node if the workflow has been configured with a cluster manager.

    Most tasks should be set to be Distributed, only extremely light weight tasks
    should be set to LOCAL.
    """
    # perhaps this should have it's own namespace
    # pbsmrtpipe.task_types.
    LOCAL = to_constant_ns('local_task')
    DISTRIBUTED = to_constant_ns('distributed_task')


class SymbolTypes(object):
    """*Symbols* that are understood durning resolving, such as max number of
    processors, Max Chunks"""
    MAX_NPROC = '$max_nproc'
    MAX_NCHUNKS = '$max_nchunks'
    TASK_TYPE = '$task_type'
    RESOLVED_OPTS = '$ropts'
    SCHEMA_OPTS = '$opts_schema'
    OPTS = '$opts'
    NCHUNKS = '$nchunks'
    NPROC = '$nproc'


class ResourceTypes(object):
    """Resources such as tmp dirs and files, log files"""
    TMP_DIR = '$tmpdir'
    TMP_FILE = '$tmpfile'
    LOG_FILE = '$logfile'
    # tasks can write output to this directory
    OUTPUT_DIR = '$outputdir'
    # Not sure this is a good idea
    #TASK_DIR = '$taskdir'

    @classmethod
    def ALL(cls):
        return cls.TMP_DIR, cls.TMP_FILE, cls.LOG_FILE, cls.OUTPUT_DIR

    @classmethod
    def is_tmp_resource(cls, name):
        return name in (cls.TMP_FILE, cls.TMP_DIR)

    @classmethod
    def is_valid(cls, attr_name):
        return attr_name in cls.ALL()


class _RegisteredFileType(type):
    def __init__(cls, name, bases, dct):
        super(_RegisteredFileType, cls).__init__(name, bases, dct)

    def __call__(cls, *args, **kwargs):
        if len(args) != 4:
            log.error(args)
            raise ValueError("Incorrect initialization for {c}".format(c=cls.__name__))

        file_type_id, base_name, file_ext, mime_type = args
        file_type = REGISTERED_FILE_TYPES.get(file_type_id, None)

        if file_type is None:
            file_type = super(_RegisteredFileType, cls).__call__(*args)
            #log.debug("Registering file type '{i}'".format(i=file_type_id))
            REGISTERED_FILE_TYPES[file_type_id] = file_type
        else:
            # print warning if base name, ext, mime type aren't the same
            attrs_names = [('base_name', base_name),
                           ('ext', file_ext),
                           ('mime_type', mime_type)]

            for attrs_name, value in attrs_names:
                v = getattr(file_type, attrs_name)
                if v != value:
                    _msg = "Attempting to register a file with a different '{x}' -> {v} (expected {y})".format(x=attrs_name, v=v, y=value)
                    log.warn(_msg)
                    warnings.warn(_msg)

        return file_type


class FileType(object):
    __metaclass__ = _RegisteredFileType

    def __init__(self, file_type_id, base_name, ext, mime_type):
        self.file_type_id = file_type_id
        self.base_name = base_name
        self.ext = ext
        self.mime_type = mime_type

        if file_type_id not in REGISTERED_FILE_TYPES:
            REGISTERED_FILE_TYPES[file_type_id] = self

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.file_type_id == other.file_type_id:
                if self.base_name == other.base_name:
                    if self.ext == other.ext:
                        return True
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.file_type_id,
                  b=self.base_name, e=self.ext)
        return "<{k} id={i} name={b}.{e} >".format(**_d)


class FileTypes(object):
    """Registry of all PacBio Files types"""
    # generic Txt file
    TXT = FileType(to_file_ns('txt'), 'file', 'txt', 'text/plain')

    # THIS NEEDS TO BE CONSISTENT with scala code. When the datastore
    # is written to disk the file type id's might be translated to
    # the DataSet style file type ids.
    REPORT = FileType(to_file_ns('JsonReport'), "report", "json", 'application/json')
    CHUNK = FileType(to_file_ns("CHUNK"), "chunk", "json", 'application/json')

    FASTA = FileType(to_file_ns('Fasta'), "file", "fasta", 'text/plain')
    FASTQ = FileType(to_file_ns('Fastq'), "file", "fastq", 'text/plain')

    # Not sure this should be a special File Type?
    INPUT_XML = FileType(to_file_ns('input_xml'), "input", "xml", 'application/xml')
    FOFN = FileType(to_file_ns("generic_fofn"), "generic", "fofn", 'text/plain')
    MOVIE_FOFN = FileType(to_file_ns('movie_fofn'), "movie", "fofn", 'text/plain')
    RGN_FOFN = FileType(to_file_ns('rgn_fofn'), "region", "fofn", 'text/plain')

    ALIGNMENT_CMP_H5 = FileType(to_file_ns('alignment_cmp_h5'), "alignments", "cmp.h5", 'application/octet-stream')
    # I am not sure this should be a first class file
    BLASR_M4 = FileType(to_file_ns('blasr_file'), 'blasr', 'm4', 'text/plain')
    BAM = FileType(to_file_ns('bam'), "alignments", "bam", 'application/octet-stream')
    BAMBAI = FileType(to_file_ns('bam_bai'), "alignments", "bam.bai", 'application/octet-stream')
    BED = FileType(to_file_ns('bed'), "file", "bed", 'text/plain')
    SAM = FileType(to_file_ns('sam'), "alignments", "sam", 'application/octet-stream')
    VCF = FileType(to_file_ns('vcf'), "file", "vcf", 'text/plain')
    GFF = FileType(to_file_ns('gff'), "file", "gff", 'text/plain')
    CSV = FileType(to_file_ns('csv'), "file", "csv", 'text/csv')
    XML = FileType(to_file_ns('xml'), "file", "xml", 'application/xml')

    # DataSet Types
    DS_SUBREADS = FileType(to_ds_ns("HdfSubreadSet"), "file", "h5.subreads.xml", "application/xml")
    DS_SUBREADS_H5 = FileType(to_ds_ns("SubreadSet"), "file", "subreads.xml", "application/xml")
    DS_REF = FileType(to_file_ns("ReferenceSet"), "file", "reference.dataset.xml", "application/xml")
    DS_BAM = FileType(to_file_ns("AlignmentSet"), "file", "aligned", "application/xml")

    RS_MOVIE_XML = FileType(to_file_ns("rs_movie_metadata"), "file", "rs_movie.metadata.xml", "application/xml")

    # this needs to not be a directory
    REF_ENTRY_XML = FileType(to_file_ns('reference_info_xml'), "reference.info.xml", "xml", 'application/xml')


