"""Core models used in the ToolContract and Resolved ToolContract


Large parts of this are pulled from pbsmrtpipe.

Author: Michael Kocher
"""
import json
import logging
import os
import re
import warnings
import functools
import datetime

log = logging.getLogger(__name__)

REGISTERED_FILE_TYPES = {}


class PacBioNamespaces(object):
    # File Types
    # PBSMRTPIPE_FILE_PREFIX = 'pbsmrtpipe.files'
    # NEW File Type Identifier style Prefix
    NEW_PBSMRTPIPE_FILE_PREFIX = "PacBio.FileTypes"
    # New DataSet Identifier Prefix
    DATASET_FILE_PREFIX = "PacBio.DataSet"

    PB_INDEX = "PacBio.Index"

    # Task Ids
    PBSMRTPIPE_TASK_PREFIX = 'pbsmrtpipe.tasks'

    PB_TASK_TYPES = 'pbsmrtpipe.task_types'

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
to_task_types_ns = functools.partial(__to_type, PacBioNamespaces.PB_TASK_TYPES)
to_workflow_option_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_OPTS_PREFIX)
to_pipeline_ns = functools.partial(__to_type, PacBioNamespaces.PBSMRTPIPE_PIPELINES)
to_index_ns = functools.partial(__to_type, PacBioNamespaces.PB_INDEX)


class TaskTypes(object):
    # This is really TC types

    STANDARD = to_task_types_ns("standard")
    SCATTERED = to_task_types_ns("scattered")
    GATHERED = to_task_types_ns("gathered")


class SymbolTypes(object):

    """*Symbols* that are understood during resolving, such as max number of
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

    @property
    def default_name(self):
        return ".".join([self.base_name, self.ext])

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
                  n=self.default_name)
        return "<{k} id={i} name={n} >".format(**_d)


class MimeTypes(object):
    JSON = 'application/json'
    TXT = 'text/plain'
    CSV = 'text/csv'
    XML = 'application/xml'
    BINARY = 'application/octet-stream'
    PICKLE = 'application/python-pickle'


class FileTypes(object):

    """Registry of all PacBio Files types

    This needs to be cleaned up and solidified. The old pre-SA3 file types need to be deleted.

    """

    # generic Txt file
    TXT = FileType(to_file_ns('txt'), 'file', 'txt', MimeTypes.TXT)
    # Generic Log file
    LOG = FileType(to_file_ns('log'), 'file', 'log', MimeTypes.TXT)

    # THIS NEEDS TO BE CONSISTENT with scala code. When the datastore
    # is written to disk the file type id's might be translated to
    # the DataSet style file type ids.
    REPORT = FileType(to_file_ns('JsonReport'), "report", "json", MimeTypes.JSON)

    # this will go away soon in favor of using a more type based model to
    # distinguish between scatter and gather file types
    CHUNK = FileType(to_file_ns("CHUNK"), "chunk", "json", MimeTypes.JSON)
    GCHUNK = FileType(to_file_ns("GCHUNK"), 'gather_chunk', "json", MimeTypes.JSON)
    SCHUNK = FileType(to_file_ns("SCHUNK"), "scatter_chunk", "json", MimeTypes.JSON)

    FASTA = FileType(to_file_ns('Fasta'), "file", "fasta", MimeTypes.TXT)
    FASTQ = FileType(to_file_ns('Fastq'), "file", "fastq", MimeTypes.TXT)

    # Not sure this should be a special File Type?
    INPUT_XML = FileType(to_file_ns('input_xml'), "input", "xml", MimeTypes.XML)
    FOFN = FileType(to_file_ns("generic_fofn"), "generic", "fofn", MimeTypes.TXT)
    MOVIE_FOFN = FileType(to_file_ns('movie_fofn'), "movie", "fofn", MimeTypes.TXT)
    RGN_FOFN = FileType(to_file_ns('rgn_fofn'), "region", "fofn", MimeTypes.TXT)

    RS_MOVIE_XML = FileType(to_file_ns("rs_movie_metadata"), "file", "rs_movie.metadata.xml", MimeTypes.XML)
    REF_ENTRY_XML = FileType(to_file_ns('reference_info_xml'), "reference.info.xml", "xml", MimeTypes.XML)

    ALIGNMENT_CMP_H5 = FileType(to_file_ns('alignment_cmp_h5'), "alignments", "cmp.h5", MimeTypes.BINARY)
    # I am not sure this should be a first class file
    BLASR_M4 = FileType(to_file_ns('blasr_file'), 'blasr', 'm4', MimeTypes.TXT)
    BAM = FileType(to_file_ns('bam'), "alignments", "bam", MimeTypes.BINARY)
    BAMBAI = FileType(to_file_ns('bam_bai'), "alignments", "bam.bai", MimeTypes.BINARY)

    BED = FileType(to_file_ns('bed'), "file", "bed", MimeTypes.TXT)
    SAM = FileType(to_file_ns('sam'), "alignments", "sam", MimeTypes.BINARY)
    VCF = FileType(to_file_ns('vcf'), "file", "vcf", MimeTypes.TXT)
    GFF = FileType(to_file_ns('gff'), "file", "gff", MimeTypes.TXT)
    CSV = FileType(to_file_ns('csv'), "file", "csv", MimeTypes.CSV)
    XML = FileType(to_file_ns('xml'), "file", "xml", 'application/xml')
    # Generic Json File
    JSON = FileType(to_file_ns("json"), "file", "json", MimeTypes.JSON)
    # Generic H5 File
    H5 = FileType(to_file_ns("h5"), "file", "h5", MimeTypes.BINARY)
    # Generic Python pickle XXX EVIL
    PICKLE = FileType(to_file_ns("pickle"), "file", "pickle", MimeTypes.PICKLE)

    # ******************* NEW SA3 File Types ********************
    # DataSet Types. The default file names should have well-defined agreed
    # upon format. See what Dave did for the bam files.
    # https://github.com/PacificBiosciences/PacBioFileFormats
    DS_SUBREADS_H5 = FileType(to_ds_ns("HdfSubreadSet"), "file", "hdfsubreadset.xml", MimeTypes.XML)
    DS_SUBREADS = FileType(to_ds_ns("SubreadSet"), "file", "subreadset.xml", MimeTypes.XML)
    DS_CCS = FileType(to_ds_ns("ConsensusReadSet"), "file", "consensusreadset.xml", MimeTypes.XML)
    DS_REF = FileType(to_ds_ns("ReferenceSet"), "file", "referenceset.xml", MimeTypes.XML)
    DS_ALIGN = FileType(to_ds_ns("AlignmentSet"), "file", "alignmentset.xml", MimeTypes.XML)
    DS_CONTIG = FileType(to_ds_ns("ContigSet"), "file", "contigset.xml", MimeTypes.XML)
    DS_BARCODE = FileType(to_ds_ns("BarcodeSet"), "file", "barcodeset.xml", MimeTypes.XML)
    DS_ALIGN_CCS = FileType(to_ds_ns("ConsensusAlignmentSet"), "file",
                            "consensusalignmentset.xml", MimeTypes.XML)

    # Index Files
    I_SAM = FileType(to_index_ns("SamIndex"), "file", "sam.index", MimeTypes.BINARY)
    I_SAW = FileType(to_index_ns("SaWriterIndex"), "file", "sa", MimeTypes.BINARY)

    # PacBio Defined Formats
    FASTA_BC = FileType("PacBio.BarcodeFile.BarcodeFastaFile", "file", "barcode.fasta", MimeTypes.TXT)
    # No ':' or '"' in the id
    FASTA_REF = FileType("PacBio.ReferenceFile.ReferenceFastaFile", "file", "pbreference.fasta", MimeTypes.TXT)

    # FIXME. Add Bax/Bam Formats here. This should replace the exiting pre-SA3 formats.
    BAM_ALN = FileType("PacBio.AlignmentFile.AlignmentBamFile", "file", "alignment.bam", MimeTypes.BINARY)
    BAM_SUB = FileType("PacBio.SubreadFile.SubreadBamFile", "file", "subread.bam", MimeTypes.BINARY)
    BAM_CCS = FileType("PacBio.ConsensusReadFile.ConsensusReadBamFile", "file", "ccs.bam", MimeTypes.BINARY)

    BAX = FileType("PacBio.SubreadFile.BaxFile", "file", "bax.h5", MimeTypes.BINARY)

    # THIS IS EXPERIMENT for internal analysis. DO NOT use
    COND = FileType(to_file_ns("COND"), "file", "conditions.json", MimeTypes.JSON)

    @staticmethod
    def is_valid_id(file_type_id):
        return file_type_id in REGISTERED_FILE_TYPES

    @staticmethod
    def ALL():
        return REGISTERED_FILE_TYPES


class DataStoreFile(object):

    def __init__(self, uuid, file_id, type_id, path):
        # adding this for consistency. In the scala code, the unique id must be
        # a uuid format
        self.uuid = uuid
        # this must globally unique. This is used to provide context to where
        # the file originated from (i.e., the tool author
        self.file_id = file_id
        # Consistent with a value in FileTypes
        self.file_type_id = type_id
        self.path = path
        self.file_size = os.path.getsize(path)
        self.created_at = datetime.datetime.fromtimestamp(os.path.getctime(path))
        self.modified_at = datetime.datetime.fromtimestamp(os.path.getmtime(path))

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.file_id,
                  t=self.file_type_id,
                  p=os.path.basename(self.path))
        return "<{k} {i} type:{t} filename:{p} >".format(**_d)

    def to_dict(self):
        return dict(sourceId=self.file_id,
                    uniqueId=str(self.uuid),
                    fileTypeId=self.file_type_id,
                    path=self.path,
                    fileSize=self.file_size,
                    createdAt=_datetime_to_string(self.created_at),
                    modifiedAt=_datetime_to_string(self.modified_at))

    @staticmethod
    def from_dict(d):
        # FIXME. This isn't quite right.
        to_a = lambda x: x.encode('ascii', 'ignore')
        to_k = lambda x: to_a(d[x])
        return DataStoreFile(to_k('uniqueId'), to_k('sourceId'), to_k('fileTypeId'), to_k('path'))


def _datetime_to_string(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


class DataStore(object):
    version = "0.2.2"

    def __init__(self, ds_files, created_at=None):
        """

        :type ds_files: list[DataStoreFile]
        """
        self.files = {f.uuid: f for f in ds_files}
        self.created_at = datetime.datetime.now() if created_at is None else created_at
        self.updated_at = datetime.datetime.now()

    def __repr__(self):
        _d = dict(n=len(self.files), k=self.__class__.__name__)
        return "<{k} nfiles={n} >".format(**_d)

    def add(self, ds_file):
        if isinstance(ds_file, DataStoreFile):
            self.files[ds_file.uuid] = ds_file
            self.updated_at = datetime.datetime.now()
        else:
            raise TypeError("DataStoreFile expected. Got type {t} for {d}".format(t=type(ds_file), d=ds_file))

    def to_dict(self):
        fs = [f.to_dict() for i, f in self.files.iteritems()]
        _d = dict(version=self.version,
                  createdAt=_datetime_to_string(self.created_at),
                  updatedAt=_datetime_to_string(self.updated_at), files=fs)
        return _d

    def _write_json(self, file_name, permission):
        with open(file_name, permission) as f:
            s = json.dumps(self.to_dict(), indent=4, sort_keys=True)
            f.write(s)

    def write_json(self, file_name):
        # if the file exists is should raise?
        self._write_json(file_name, 'w')

    def write_update_json(self, file_name):
        """Overwrite Datastore with current state"""
        self._write_json(file_name, 'w+')

    @staticmethod
    def load_from_json(path):
        with open(path, 'r') as reader:
            d = json.loads(reader.read())

        ds_files = [DataStoreFile.from_dict(x) for x in d['files']]
        return DataStore(ds_files)


def _is_chunk_key(k):
    return k.startswith(PipelineChunk.CHUNK_KEY_PREFIX)


class MalformedChunkKeyError(ValueError):

    """Chunk Key does NOT adhere to the spec"""
    pass


class PipelineChunk(object):

    CHUNK_KEY_PREFIX = "$chunk."
    RX_CHUNK_KEY = re.compile(r'^\$chunk\.([A-z0-9_]*)')

    def __init__(self, chunk_id, **kwargs):
        """

        kwargs is a key-value store. keys that begin "$chunk." are considered
        to be semantically understood by workflow and can be "routed" to
        chunked task inputs.

        Values that don't begin with "$chunk." are considered metadata.


        :param chunk_id: Chunk id
        :type chunk_id: str

        """
        if self.RX_CHUNK_KEY.match(chunk_id) is not None:
            raise MalformedChunkKeyError("'{c}' expected {p}".format(c=chunk_id, p=self.RX_CHUNK_KEY.pattern))

        self.chunk_id = chunk_id
        # loose key-value pair
        self._datum = kwargs

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.chunk_id, c=",".join(self.chunk_keys))
        return "<{k} id='{i}' chunk keys={c} >".format(**_d)

    def set_chunk_key(self, chunk_key, value):
        """Overwrite or add a chunk_key => value to the Chunk datum

        the chunk-key can be provided with or without the '$chunk:' prefix
        """
        if not chunk_key.startswith(PipelineChunk.CHUNK_KEY_PREFIX):
            chunk_key = PipelineChunk.CHUNK_KEY_PREFIX + chunk_key
        self._datum[chunk_key] = value

    def set_metadata_key(self, metadata_key, value):
        """Set chunk metadata key => value

        metadata key must NOT begin with $chunk. format
        """
        if metadata_key.startswith(PipelineChunk.CHUNK_KEY_PREFIX):
            raise ValueError("Cannot set chunk-key values. {i}".format(i=metadata_key))
        self._datum[metadata_key] = value

    @property
    def chunk_d(self):
        return {k: v for k, v in self._datum.iteritems() if _is_chunk_key(k)}

    @property
    def chunk_keys(self):
        return self.chunk_d.keys()

    @property
    def chunk_metadata(self):
        return {k: v for k, v in self._datum.iteritems() if not _is_chunk_key(k)}

    def to_dict(self):
        return {'chunk_id': self.chunk_id, 'chunk': self._datum}
