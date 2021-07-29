"""
Core models used in the ToolContract and Resolved ToolContract
Large parts of this are pulled from pbsmrtpipe.

Author: Michael Kocher
"""

import datetime
import functools
import json
import logging
import os
import re
import traceback
import types
import uuid
import warnings
from collections import namedtuple, OrderedDict

from pbcommand import to_ascii

log = logging.getLogger(__name__)

REGISTERED_FILE_TYPES = {}

# Light weight Dataset metatadata. Use pbcore for full dataset functionality
DataSetMetaData = namedtuple("DataSetMetaData", 'uuid metatype')


class PacBioNamespaces:
    # File Types
    # NEW File Type Identifier style Prefix
    NEW_FILE_PREFIX = "PacBio.FileTypes"
    # New DataSet Identifier Prefix
    DATASET_FILE_PREFIX = "PacBio.DataSet"

    PB_INDEX = "PacBio.Index"


def __to_type(prefix, name):
    return ".".join([prefix, name])


to_file_ns = functools.partial(__to_type, PacBioNamespaces.NEW_FILE_PREFIX)
to_ds_ns = functools.partial(__to_type, PacBioNamespaces.DATASET_FILE_PREFIX)
to_index_ns = functools.partial(__to_type, PacBioNamespaces.PB_INDEX)


class TaskOptionTypes:
    """Core Task Option type id type"""

    INT = "integer"
    BOOL = "boolean"
    STR = "string"
    FLOAT = "float"
    FILE = "file"
    # Choice type Options
    CHOICE_STR = "choice_string"
    CHOICE_INT = "choice_integer"
    CHOICE_FLOAT = "choice_float"

    @classmethod
    def ALL(cls):
        """Return a set of all Task Option Types"""
        return {cls.INT, cls.BOOL, cls.STR, cls.FLOAT, cls.CHOICE_STR,
                cls.CHOICE_INT, cls.CHOICE_FLOAT, cls.FILE}

    @classmethod
    def _raise_value_error(cls, value, allowed, option_type_name):
        raise ValueError("Incompatible task {o} option type id '{s}'. "
                         "Allowed values {v}".format(o=option_type_name,
                                                     s=value,
                                                     v=",".join(allowed)))

    @classmethod
    def ALL_SIMPLE(cls):
        """Returns a set of 'simple' task option types (e.g., boolean, string, int, float)"""
        return {cls.STR, cls.BOOL, cls.INT, cls.FLOAT, cls.FILE}

    @classmethod
    def from_simple_str(cls, sx):
        """Validates a string is a validate task option type id or raise ValueError

        :raises ValueError
        """
        if sx in cls.ALL_SIMPLE():
            return sx
        else:
            cls._raise_value_error(sx, cls.ALL_SIMPLE(), "simple")

    @classmethod
    def ALL_CHOICES(cls):
        """Returns a set of choice task option types"""
        return {cls.CHOICE_INT, cls.CHOICE_FLOAT, cls.CHOICE_STR}

    @classmethod
    def is_choice(cls, sx):
        return sx in cls.ALL_CHOICES()

    @classmethod
    def from_choice_str(cls, sx):
        """Validates and returns a task choice option type or raises ValueError"""
        if sx in cls.ALL_CHOICES():
            return sx
        else:
            cls._raise_value_error(sx, cls.ALL_CHOICES(), "choice")

    @classmethod
    def from_str(cls, sx):
        """Validates and returns a valid type option type id or raises ValueError,

        :note: For legacy reasons, "number" will be mapped to "float"
        """
        # FIXME, Legacy fix, "number" appears to mean "float"?
        if sx == "number":
            sx = TaskOptionTypes.FLOAT

        if sx in TaskOptionTypes.ALL():
            return sx
        else:
            cls._raise_value_error(sx, cls.ALL(), "")

    @classmethod
    def from_any(cls, val):
        if isinstance(val, bool):
            return cls.BOOL
        elif isinstance(val, int):
            return cls.INT
        elif isinstance(val, float):
            return cls.FLOAT
        elif val is None:  # XXX special case
            return cls.FILE
        return cls.STR


class SymbolTypes:
    """
    *Symbols* that are understood during resolving, such as max number of
    processors, Max Chunks. Used when defining a Tool Contract
    """
    MAX_NPROC = '$max_nproc'
    MAX_NCHUNKS = '$max_nchunks'
    TASK_TYPE = '$task_type'
    RESOLVED_OPTS = '$ropts'
    SCHEMA_OPTS = '$opts_schema'
    OPTS = '$opts'
    NCHUNKS = '$nchunks'
    NPROC = '$nproc'


class ResourceTypes:
    """
    Resources such as tmp dirs and files, log files. Used when defining
    a Tool Contract
    """
    TMP_DIR = '$tmpdir'
    TMP_FILE = '$tmpfile'
    LOG_FILE = '$logfile'
    # tasks can write output to this directory
    OUTPUT_DIR = '$outputdir'
    # Not sure this is a good idea
    # TASK_DIR = '$taskdir'

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
        super().__init__(name, bases, dct)

    def __call__(cls, *args, **kwargs):
        if len(args) != 4:
            log.error(args)
            raise ValueError(
                "Incorrect initialization for {c}".format(
                    c=cls.__name__))

        file_type_id, base_name, file_ext, mime_type = args
        file_type = REGISTERED_FILE_TYPES.get(file_type_id, None)

        if file_type is None:
            file_type = super().__call__(*args)
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
                    _msg = "Attempting to register a file with a different '{x}' -> {v} (expected {y})".format(
                        x=attrs_name, v=v, y=value)
                    log.warn(_msg)
                    warnings.warn(_msg)

        return file_type


class FileType(metaclass=_RegisteredFileType):
    def __init__(self, file_type_id, base_name, ext, mime_type):
        """
        Core File Type data model

        :param file_type_id: unique file string
        :param base_name: default base name of the file (without extension)
        :param ext: file extension
        :param mime_type:  file mimetype
        :return:
        """
        self.file_type_id = file_type_id
        self.base_name = base_name
        self.ext = ext
        self.mime_type = mime_type

        if file_type_id not in REGISTERED_FILE_TYPES:
            REGISTERED_FILE_TYPES[file_type_id] = self

    @property
    def default_name(self):
        """ Default name of file alias for base_name"""
        return self.base_name  # ".".join([self.base_name, self.ext])

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if self.file_type_id == other.file_type_id:
                if self.base_name == other.base_name:
                    if self.ext == other.ext:
                        return True
        return False

    def __repr__(self):
        _d = dict(k=self.__class__.__name__,
                  i=self.file_type_id,
                  n=self.default_name)
        return "<{k} id={i} name={n} >".format(**_d)


class DataSetFileType(FileType):
    """File types that are a DataSet Type"""
    pass


class MimeTypes:
    """Supported Mime types"""
    JSON = 'application/json'
    # This might be better as 'application/svg+xml' ?
    SVG = 'image/svg+xml'
    TXT = 'text/plain'
    CSV = 'text/csv'
    HTML = 'text/html'
    XML = 'application/xml'
    BINARY = 'application/octet-stream'
    PICKLE = 'application/python-pickle'
    GZIP = 'application/x-gzip'
    ZIP = 'application/zip'


class FileTypes:

    """Registry of all PacBio Files types

    This needs to be cleaned up and solidified. The old pre-SA3 file types need to be deleted.

    """

    # generic Txt file
    TXT = FileType(to_file_ns('txt'), 'file', 'txt', MimeTypes.TXT)
    # Generic Log file
    LOG = FileType(to_file_ns('log'), 'file', 'log', MimeTypes.TXT)
    # Config file
    CFG = FileType(to_file_ns('cfg'), 'config', 'cfg', MimeTypes.TXT)

    SVG = FileType(to_file_ns('svg'), "file", 'svg', MimeTypes.SVG)

    # THIS NEEDS TO BE CONSISTENT with scala code. When the datastore
    # is written to disk the file type id's might be translated to
    # the DataSet style file type ids.
    REPORT = FileType(
        to_file_ns('JsonReport'),
        "report",
        "json",
        MimeTypes.JSON)
    DATASTORE = FileType(
        to_file_ns("Datastore"),
        "file",
        "datastore.json",
        MimeTypes.JSON)

    # this will go away soon in favor of using a more type based model to
    # distinguish between scatter and gather file types
    CHUNK = FileType(to_file_ns("CHUNK"), "chunk", "json", MimeTypes.JSON)
    GCHUNK = FileType(
        to_file_ns("GCHUNK"),
        'gather_chunk',
        "json",
        MimeTypes.JSON)
    SCHUNK = FileType(
        to_file_ns("SCHUNK"),
        "scatter_chunk",
        "json",
        MimeTypes.JSON)

    FASTA = FileType(to_file_ns('Fasta'), "file", "fasta", MimeTypes.TXT)
    FASTQ = FileType(to_file_ns('Fastq'), "file", "fastq", MimeTypes.TXT)

    # Not sure this should be a special File Type?
    INPUT_XML = FileType(
        to_file_ns('input_xml'),
        "input",
        "xml",
        MimeTypes.XML)
    FOFN = FileType(
        to_file_ns("generic_fofn"),
        "generic",
        "fofn",
        MimeTypes.TXT)
    MOVIE_FOFN = FileType(
        to_file_ns('movie_fofn'),
        "movie",
        "fofn",
        MimeTypes.TXT)
    RGN_FOFN = FileType(
        to_file_ns('rgn_fofn'),
        "region",
        "fofn",
        MimeTypes.TXT)

    RS_MOVIE_XML = FileType(
        to_file_ns("rs_movie_metadata"),
        "file",
        "rs_movie.metadata.xml",
        MimeTypes.XML)
    REF_ENTRY_XML = FileType(
        to_file_ns('reference_info_xml'),
        "reference.info.xml",
        "xml",
        MimeTypes.XML)

    ALIGNMENT_CMP_H5 = FileType(
        to_file_ns('alignment_cmp_h5'),
        "alignments",
        "cmp.h5",
        MimeTypes.BINARY)
    # I am not sure this should be a first class file
    BLASR_M4 = FileType(to_file_ns('blasr_file'), 'blasr', 'm4', MimeTypes.TXT)
    BAM = FileType(to_file_ns('bam'), "alignments", "bam", MimeTypes.BINARY)
    BAMBAI = FileType(
        to_file_ns('bam_bai'),
        "alignments",
        "bam.bai",
        MimeTypes.BINARY)

    BED = FileType(to_file_ns('bed'), "file", "bed", MimeTypes.TXT)
    SAM = FileType(to_file_ns('sam'), "alignments", "sam", MimeTypes.BINARY)
    VCF = FileType(to_file_ns('vcf'), "file", "vcf", MimeTypes.TXT)
    GFF = FileType(to_file_ns('gff'), "file", "gff", MimeTypes.TXT)
    BIGWIG = FileType(
        to_file_ns('bigwig'),
        "annotations",
        "bw",
        MimeTypes.BINARY)
    CSV = FileType(to_file_ns('csv'), "file", "csv", MimeTypes.CSV)
    XML = FileType(to_file_ns('xml'), "file", "xml", MimeTypes.XML)
    HTML = FileType(to_file_ns('html'), "file", "html", MimeTypes.HTML)
    # Generic Json File
    JSON = FileType(to_file_ns("json"), "file", "json", MimeTypes.JSON)
    # Generic H5 File
    H5 = FileType(to_file_ns("h5"), "file", "h5", MimeTypes.BINARY)
    # Generic Python pickle XXX EVIL
    PICKLE = FileType(to_file_ns("pickle"), "file", "pickle", MimeTypes.PICKLE)
    # GZIPped archive
    GZIP = FileType(to_file_ns("gzip"), "file", "gz", MimeTypes.GZIP)
    TGZ = FileType(to_file_ns("tgz"), "file", "tar.gz", MimeTypes.GZIP)
    ZIP = FileType(to_file_ns("zip"), "file", "zip", MimeTypes.ZIP)

    # ******************* NEW SA3 File Types ********************
    # DataSet Types. The default file names should have well-defined agreed
    # upon format. See what Dave did for the bam files.
    # https://github.com/PacificBiosciences/PacBioFileFormats
    DS_SUBREADS_H5 = DataSetFileType(
        to_ds_ns("HdfSubreadSet"),
        "file",
        "hdfsubreadset.xml",
        MimeTypes.XML)
    DS_SUBREADS = DataSetFileType(
        to_ds_ns("SubreadSet"),
        "file",
        "subreadset.xml",
        MimeTypes.XML)
    DS_CCS = DataSetFileType(
        to_ds_ns("ConsensusReadSet"),
        "file",
        "consensusreadset.xml",
        MimeTypes.XML)
    DS_REF = DataSetFileType(
        to_ds_ns("ReferenceSet"),
        "file",
        "referenceset.xml",
        MimeTypes.XML)
    DS_ALIGN = DataSetFileType(
        to_ds_ns("AlignmentSet"),
        "file",
        "alignmentset.xml",
        MimeTypes.XML)
    DS_CONTIG = DataSetFileType(
        to_ds_ns("ContigSet"),
        "file",
        "contigset.xml",
        MimeTypes.XML)
    DS_BARCODE = DataSetFileType(
        to_ds_ns("BarcodeSet"),
        "file",
        "barcodeset.xml",
        MimeTypes.XML)
    DS_ALIGN_CCS = DataSetFileType(to_ds_ns("ConsensusAlignmentSet"), "file",
                                   "consensusalignmentset.xml", MimeTypes.XML)
    DS_GMAP_REF = DataSetFileType(to_ds_ns("GmapReferenceSet"), "file",
                                  "gmapreferenceset.xml", MimeTypes.XML)
    DS_TRANSCRIPT = DataSetFileType(to_ds_ns("TranscriptSet"), "file",
                                    "transcriptset.xml", MimeTypes.XML)
    DS_ALIGN_TRANSCRIPT = DataSetFileType(
        to_ds_ns("TranscriptAlignmentSet"),
        "file",
        "transcriptalignmentset.xml",
        MimeTypes.XML)

    # PacBio Defined Formats
    # **** Index Files

    # ReferenceSet specific
    I_SAM = FileType(
        to_index_ns("SamIndex"),
        "file",
        "sam.index",
        MimeTypes.BINARY)
    I_SAW = FileType(
        to_index_ns("SaWriterIndex"),
        "file",
        "sa",
        MimeTypes.BINARY)

    # SMRT VIew specific files
    I_INDEXER = FileType(
        to_index_ns("Indexer"),
        "file",
        "fasta.index",
        MimeTypes.TXT)
    I_FCI = FileType(
        to_index_ns("FastaContigIndex"),
        "file",
        "fasta.contig.index",
        MimeTypes.TXT)

    # PacBio BAM pbi
    I_PBI = FileType(
        to_index_ns("PacBioIndex"),
        "file",
        "pbi",
        MimeTypes.BINARY)
    # This is duplicated from the old pre-DS era models. see BAMBAI
    I_BAI = FileType(
        to_index_ns("BamIndex"),
        "file",
        "bam.bai",
        MimeTypes.BINARY)

    # NGMLR indices
    I_NGMLR_ENC = FileType(
        to_index_ns("NgmlrRefEncoded"),
        "file",
        ".ngm",
        MimeTypes.BINARY)
    I_NGMLR_TAB = FileType(
        to_index_ns("NgmlrRefTable"),
        "file",
        ".ngm",
        MimeTypes.BINARY)

    # Fasta type files
    FASTA_BC = FileType(
        "PacBio.BarcodeFile.BarcodeFastaFile",
        "file",
        "barcode.fasta",
        MimeTypes.TXT)
    # No ':' or '"' in the id
    FASTA_REF = FileType(
        "PacBio.ReferenceFile.ReferenceFastaFile",
        "file",
        "pbreference.fasta",
        MimeTypes.TXT)
    CONTIG_FA = FileType(
        "PacBio.ContigFile.ContigFastaFile",
        "file",
        "contig.fasta",
        MimeTypes.TXT)

    # Adapter Fasta File From PPA
    FASTA_ADAPTER = FileType(
        "PacBio.SubreadFile.AdapterFastaFile",
        "file",
        "adapters.fasta",
        MimeTypes.TXT)
    FASTA_CONTROL = FileType(
        "PacBio.SubreadFile.ControlFastaFile",
        "file",
        "control.fasta",
        MimeTypes.TXT)

    # BAM dialects
    BAM_ALN = FileType(
        "PacBio.AlignmentFile.AlignmentBamFile",
        "file",
        "alignment.bam",
        MimeTypes.BINARY)
    BAM_SUB = FileType(
        "PacBio.SubreadFile.SubreadBamFile",
        "file",
        "subread.bam",
        MimeTypes.BINARY)
    BAM_CCS = FileType(
        "PacBio.ConsensusReadFile.ConsensusReadBamFile",
        "file",
        "ccs.bam",
        MimeTypes.BINARY)
    BAM_CCS_ALN = FileType(
        "PacBio.AlignmentFile.ConsensusAlignmentBamFile",
        "file",
        "ccs_align.bam",
        MimeTypes.BINARY)
    BAM_TRANSCRIPT = FileType(
        "PacBio.TranscriptFile.TranscriptBamFile",
        "file",
        "transcripts.bam",
        MimeTypes.BINARY)
    BAM_TRANSCRIPT_ALN = FileType(
        "PacBio.AlignmentFile.TranscriptAlignmentBamFile",
        "file",
        "transcripts_align.bam",
        MimeTypes.BINARY)
    # MK TODO. Add remaining SubreadSet files types, Scraps, HqRegion, etc..

    BAZ = FileType("PacBio.ReadFile.BazFile", "file", "baz", MimeTypes.BINARY)
    TRC = FileType(
        "PacBio.ReadFile.TraceFile",
        "file",
        "trc",
        MimeTypes.BINARY)
    PLS = FileType(
        "PacBio.ReadFile.PulseFile",
        "file",
        "pls",
        MimeTypes.BINARY)
    # RS era
    BAX = FileType(
        "PacBio.SubreadFile.BaxFile",
        "file",
        "bax.h5",
        MimeTypes.BINARY)

    # sts.xml
    STS_XML = FileType("PacBio.SubreadFile.ChipStatsFile",
                       "file", "sts.xml", MimeTypes.XML)
    STS_H5 = FileType("PacBio.SubreadFile.ChipStatsH5File",
                      "file", "sts.h5", MimeTypes.BINARY)

    # Resequencing Conditions File Format
    COND_RESEQ = FileType(
        to_file_ns("COND_RESEQ"),
        "file",
        "conditions-reseq.json",
        MimeTypes.JSON)

    @staticmethod
    def is_valid_id(file_type_id):
        return file_type_id in REGISTERED_FILE_TYPES

    @staticmethod
    def ALL_DATASET_TYPES():
        return {i: f for i, f in REGISTERED_FILE_TYPES.items()
                if isinstance(f, DataSetFileType)}

    @staticmethod
    def ALL():
        """Returns a Dict of id->FileType

        :rtype dict[str, FileType]
        """
        return REGISTERED_FILE_TYPES


def _get_timestamp_or_now(path, func):
    if os.path.exists(path):
        return func(path)
    else:
        return datetime.datetime.now()


def _get_file_path(path, base_path=None):
    if base_path is None or os.path.isabs(path):
        return path
    else:
        return os.path.join(base_path, path)


class DataStoreFile:

    def __init__(self, uuid, source_id, type_id, path, is_chunked=False,
                 name="", description=""):
        """

        :param uuid: UUID of the datstore file
        :param source_id: source id of the DataStore file
        :param type_id: File Type id of
        :param path: Absolute path to the datastore file
        :param is_chunked: is the datastore file a "chunked" file from a scatter/chunking task
        :param name: Display name of datastore file
        :param description: Description of the datastore file

        """

        # adding this for consistency. In the scala code, the unique id must be
        # a uuid format
        self.uuid = uuid
        # this must globally unique. This is used to provide context to where
        # the file originated from (i.e., the tool author
        # This should be deprecated. Use source_id to be consistent with the
        # rest of the code base
        self.file_id = source_id
        # Consistent with a value in FileTypes
        self.file_type_id = type_id
        self.path = path
        # FIXME(mkocher)(2016-2-23): This is probably not the best model
        self.file_size = os.path.getsize(path) if os.path.exists(path) else 0
        self.created_at = _get_timestamp_or_now(
            path, lambda px: datetime.datetime.fromtimestamp(
                os.path.getctime(px)))
        self.modified_at = _get_timestamp_or_now(
            path, lambda px: datetime.datetime.fromtimestamp(
                os.path.getmtime(px)))
        # Was the file produced by Chunked task
        self.is_chunked = is_chunked
        self.name = name
        self.description = description

    @property
    def source_id(self):
        """This is the consistent form that is used in the code base"""
        return self.file_id

    @property
    def file_type(self):
        return REGISTERED_FILE_TYPES[self.file_type_id]

    def __repr__(self):
        u = str(self.uuid)[:6]
        _d = dict(k=self.__class__.__name__,
                  i=self.file_id,
                  t=self.file_type_id,
                  p=os.path.basename(self.path), u=u)
        return "<{k} {i} type:{t} filename:{p} uuid:{u} ... >".format(**_d)

    def to_dict(self):
        return dict(sourceId=self.file_id,
                    uniqueId=str(self.uuid),
                    fileTypeId=self.file_type_id,
                    path=self.path,
                    fileSize=self.file_size,
                    createdAt=_datetime_to_string(self.created_at),
                    modifiedAt=_datetime_to_string(self.modified_at),
                    isChunked=self.is_chunked,
                    name=self.name,
                    description=self.description)

    @staticmethod
    def from_dict(d, base_path=None):
        # FIXME. This isn't quite right.
        to_a = to_ascii

        def to_k(x):
            return to_a(d[x])

        is_chunked = d.get('isChunked', False)
        return DataStoreFile(to_k('uniqueId'),
                             to_k('sourceId'),
                             to_k('fileTypeId'),
                             _get_file_path(to_k('path'), base_path),
                             is_chunked=is_chunked,
                             name=to_a(d.get("name", "")),
                             description=to_a(d.get("description", "")))


def _datetime_to_string(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%S')


class DataStore:
    version = "0.2.2"

    def __init__(self, ds_files, created_at=None):
        """
        :param ds_files: list of datastore file instances
        :param created_at: Date the datastore was created. if None, will use the current datetime

        :type ds_files: list[DataStoreFile]
        """
        self.files = OrderedDict([(f.uuid, f) for f in ds_files])
        self.created_at = datetime.datetime.now() if created_at is None else created_at
        self.updated_at = datetime.datetime.now()

    def __repr__(self):
        _d = dict(n=len(self.files), k=self.__class__.__name__)
        return "<{k} nfiles={n} >".format(**_d)

    def add(self, ds_file):
        if isinstance(ds_file, DataStoreFile):
            if ds_file.uuid in self.files:
                log.warn("UUID {u} in file {s} already present in datastore (source: {f})".format(
                         u=ds_file.uuid, s=ds_file.file_id, f=self.files[ds_file.uuid].file_id))
            self.files[ds_file.uuid] = ds_file
            self.updated_at = datetime.datetime.now()
        else:
            raise TypeError(
                "DataStoreFile expected. Got type {t} for {d}".format(
                    t=type(ds_file), d=ds_file))

    def to_dict(self):
        fs = [f.to_dict() for i, f in self.files.items()]
        _d = dict(version=self.version,
                  createdAt=_datetime_to_string(self.created_at),
                  updatedAt=_datetime_to_string(self.updated_at), files=fs)
        return _d

    def write_json(self, file_name):
        write_dict_to_json(self.to_dict(), file_name, "w")

    def write_update_json(self, file_name):
        """Overwrite Datastore with current state"""
        write_dict_to_json(self.to_dict(), file_name, "w+")

    @staticmethod
    def load_from_d(d, base_path=None):
        """Load DataStore from a dict"""
        ds_files = [DataStoreFile.from_dict(x, base_path) for x in d['files']]
        return DataStore(ds_files)

    @staticmethod
    def load_from_json(path):
        """Load DataStore from a JSON file"""
        base_path = os.path.dirname(os.path.abspath(path))
        with open(path, 'r') as reader:
            d = json.loads(reader.read())
        return DataStore.load_from_d(d, base_path)


def _is_chunk_key(k):
    return k.startswith(PipelineChunk.CHUNK_KEY_PREFIX)


class MalformedChunkKeyError(ValueError):

    """Chunk Key does NOT adhere to the spec"""
    pass


class PipelineChunk:

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
            raise MalformedChunkKeyError(
                "'{c}' expected {p}".format(
                    c=chunk_id, p=self.RX_CHUNK_KEY.pattern))

        self.chunk_id = chunk_id
        # loose key-value pair
        self._datum = kwargs

    def __repr__(self):
        _d = dict(
            k=self.__class__.__name__,
            i=self.chunk_id,
            c=",".join(
                self.chunk_keys))
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
            raise ValueError(
                "Cannot set chunk-key values. {i}".format(i=metadata_key))
        self._datum[metadata_key] = value

    @property
    def chunk_d(self):
        return {k: v for k, v in self._datum.items() if _is_chunk_key(k)}

    @property
    def chunk_keys(self):
        return list(self.chunk_d.keys())

    @property
    def chunk_metadata(self):
        return {k: v for k, v in self._datum.items() if not _is_chunk_key(k)}

    def to_dict(self):
        return {'chunk_id': self.chunk_id, 'chunk': self._datum}


class DataStoreViewRule:
    """
    Rule specifying if and how the UI should display a datastore file.
    """

    def __init__(self, source_id, file_type_id, is_hidden, name="",
                 description="", type_name=None):
        """
        :param source_id: Unique source id of the datastore file
        :param file_type_id: File Type id of the datastore file
        :param is_hidden: Mark the file has hidden
        :param name: Display name of the file
        :param description: Description of the file
        :param type_name: Name of file type to display
        """

        # for generating rules compositionally in Python, it's easier to just
        # pass the FileType object directly
        if isinstance(file_type_id, FileType):
            file_type_id = file_type_id.file_type_id
        assert FileTypes.is_valid_id(file_type_id), file_type_id
        self.source_id = source_id
        self.file_type_id = file_type_id
        self.is_hidden = is_hidden
        self.name = name
        self.description = description
        self.type_name = type_name

    def to_dict(self):
        d = {"sourceId": self.source_id, "fileTypeId": self.file_type_id,
             "isHidden": self.is_hidden, "name": self.name,
             "description": self.description}
        if self.type_name is not None:  # XXX workaround for Scala code
            d["typeName"] = self.type_name
        return d

    @staticmethod
    def from_dict(d):
        return DataStoreViewRule(d['sourceId'], d['fileTypeId'], d['isHidden'],
                                 d.get('name', ''), d.get('description', ''),
                                 d.get('typeName', None))


class PipelineDataStoreViewRules:
    """
    A collection of DataStoreViewRule objects associated with a pipeline.
    """

    def __init__(self, pipeline_id, smrtlink_version, rules=()):
        self.pipeline_id = pipeline_id
        self.smrtlink_version = smrtlink_version
        self.rules = list(rules)

    def to_dict(self):
        return {"pipelineId": self.pipeline_id,
                "smrtLinkVersion": self.smrtlink_version,
                "rules": [r.to_dict() for r in self.rules]}

    @staticmethod
    def from_dict(d):
        return PipelineDataStoreViewRules(
            pipeline_id=d['pipelineId'],
            smrtlink_version=d['smrtLinkVersion'],
            rules=[DataStoreViewRule.from_dict(r) for r in d['rules']])

    @staticmethod
    def load_from_json(path):
        with open(path, 'r') as reader:
            d = json.loads(reader.read())
        return PipelineDataStoreViewRules.from_dict(d)

    def write_json(self, file_name):
        write_dict_to_json(self.to_dict(), file_name)


def write_dict_to_json(d, file_name, permission="w"):
    with open(file_name, permission) as f:
        s = json.dumps(d, indent=4, sort_keys=True,
                       separators=(',', ': '))
        f.write(s)


RX_TASK_ID = re.compile(r'^([A-z0-9_]*)\.tasks\.([A-z0-9_]*)$')
RX_TASK_OPTION_ID = re.compile(r'^([A-z0-9_\.]*)')


def _validate_id(prog, idtype, tid):
    if prog.match(tid):
        return tid
    else:
        raise ValueError(
            "Invalid format {t}: '{i}' {p}".format(
                t=idtype, i=tid, p=repr(
                    prog.pattern)))


validate_task_id = functools.partial(_validate_id, RX_TASK_ID, 'task id')
validate_task_option_id = functools.partial(_validate_id, RX_TASK_OPTION_ID,
                                            'task option id')


class BasePacBioOption:
    # This is an abstract class. This really blurring the abstract with
    # implementation which makes the interface unclear.

    # This MUST be a validate TaskOptionTypes.* value.
    OPTION_TYPE_ID = "UNKNOWN"

    @classmethod
    def validate_core_type(cls, value):
        """
        Every Option has a "core" type that needs to validated in the
        constructor. The function should return the value

        Subclasses should implement

        :param value: Option value
        :return: validated value
        """

        raise NotImplementedError

    def validate_option(self, value):
        """Core method used externally (e.g., resolvers) to validate option

        The default implementation will only validate that the "core" type
        is consistent with definition.

        Subclasses should override this to leverage internal state (e.g, self.choices)
        """
        return self.validate_core_type(value)

    def __init__(self, option_id, name, default, description):
        """
        Core constructor for the PacBio Task Option.

        :param option_id: PacBio Task Option type id. Must adhere to the A-z0-9_
        :param name: Display name of the Task Option
        :param default: Default value
        :param description: Description of the Task Option

        :type option_id: str
        :type name: str
        :type description: str
        """
        self.option_id = validate_task_option_id(option_id)
        self.name = name
        self._default = self.validate_core_type(default)
        self.description = description

        # make sure subclasses have overwritten the OPTION_TYPE_ID.
        # this will raise
        if self.OPTION_TYPE_ID not in TaskOptionTypes.ALL():
            msg = "InValid Task Option type id {t} Subclasses of {c} must " \
                  "override OPTION_TYPE_ID to have a consistent value with " \
                  "TaskOptionTypes.*".format(t=self.OPTION_TYPE_ID,
                                             c=self.__class__.__name__)
            raise ValueError(msg)

    @property
    def default(self):
        """Returns the default value for the option"""
        return self._default

    def __repr__(self):
        _d = dict(i=self.option_id,
                  n=self.name,
                  v=self.default,
                  k=self.__class__.__name__,
                  t=self.OPTION_TYPE_ID)
        return "<{k} {i} name: {n} default: {v} type:{t} >".format(**_d)

    def to_dict(self):
        option_type = TaskOptionTypes.from_str(self.OPTION_TYPE_ID)
        # the same model is used in the pipeline template, so we break the
        # snake case in favor of camelcase for the option type id.
        return dict(id=self.option_id,
                    name=self.name,
                    default=self.default,
                    description=self.description,
                    optionTypeId=option_type)


def _type_error_msg(value, expected_type):
    return "{v} Expected {t}, got {x}".format(
        v=value, t=expected_type, x=type(value))


def _strict_validate_int_or_raise(value):

    def _to_msg(type_):
        return _type_error_msg(value, type_)

    if isinstance(value, bool):
        raise TypeError(_to_msg(bool))
    elif isinstance(value, float):
        raise TypeError(_to_msg(float))
    elif isinstance(value, str):
        raise TypeError(_to_msg(str))
    else:
        return int(value)


def _strict_validate_bool_or_raise(value):
    if isinstance(value, bool):
        return value
    raise TypeError(_type_error_msg(value, bool))


def _strict_validate_float_or_raise(value):

    def _to_msg(type_):
        return _type_error_msg(value, type_)

    if isinstance(value, bool):
        raise TypeError(_to_msg(bool))
    elif isinstance(value, str):
        raise TypeError(_to_msg(str))
    else:
        return float(value)


def _strict_validate_string_or_raise(value):
    # Not supporting unicode in python2.
    if isinstance(value, str):
        return value
    raise TypeError(_type_error_msg(value, str))


def _strict_validate_file_or_raise(value):
    # Not supporting unicode in python2.
    if isinstance(value, str) or value is None:
        return value
    raise TypeError(_type_error_msg(value, str))


class PacBioIntOption(BasePacBioOption):
    OPTION_TYPE_ID = TaskOptionTypes.INT

    @classmethod
    def validate_core_type(cls, value):
        return _strict_validate_int_or_raise(value)


class PacBioFloatOption(BasePacBioOption):
    OPTION_TYPE_ID = TaskOptionTypes.FLOAT

    @classmethod
    def validate_core_type(cls, value):
        return _strict_validate_float_or_raise(value)


class PacBioBooleanOption(BasePacBioOption):
    OPTION_TYPE_ID = TaskOptionTypes.BOOL

    @classmethod
    def validate_core_type(cls, value):
        return _strict_validate_bool_or_raise(value)


class PacBioStringOption(BasePacBioOption):
    OPTION_TYPE_ID = TaskOptionTypes.STR

    @classmethod
    def validate_core_type(cls, value):
        return _strict_validate_string_or_raise(value)


class PacBioFileOption(BasePacBioOption):
    OPTION_TYPE_ID = TaskOptionTypes.FILE

    @classmethod
    def validate_core_type(cls, value):
        return _strict_validate_file_or_raise(value)


def _strict_validate_default_and_choices(core_type_validator_func):
    """

    :param core_type_validator_func: Function (value) => value or raises TypeError

    Returns a func of (value, choices) => value, choices or raises TypeError
    or Value Error.
    """
    def wrap(value, choices):
        for choice in choices:
            core_type_validator_func(choice)
        v = core_type_validator_func(value)
        if v not in choices:
            raise ValueError(
                "Default value {v} is not in allowed choices {c}".format(
                    v=value, c=choices))
        return v, choices
    return wrap


_strict_validate_int_choices = _strict_validate_default_and_choices(
    _strict_validate_int_or_raise)
_strict_validate_str_choices = _strict_validate_default_and_choices(
    _strict_validate_string_or_raise)
_strict_validate_bool_choices = _strict_validate_default_and_choices(
    _strict_validate_bool_or_raise)
_strict_validate_float_choices = _strict_validate_default_and_choices(
    _strict_validate_float_or_raise)


class BaseChoiceType(BasePacBioOption):

    # This really should be Abstract
    def __init__(self, option_id, name, default, description, choices):
        super().__init__(
            option_id,
            name,
            default,
            description)
        _, validated_choices = self.validate_core_type_with_choices(
            default, choices)
        self.choices = validated_choices

    @classmethod
    def validate_core_type_with_choices(cls, value, choices):
        raise NotImplementedError

    def validate_option(self, value):
        v, _ = self.validate_core_type_with_choices(value, self.choices)
        return v

    def to_dict(self):
        d = super().to_dict()
        d['choices'] = self.choices
        return d


class PacBioIntChoiceOption(BaseChoiceType):
    OPTION_TYPE_ID = TaskOptionTypes.CHOICE_INT

    @classmethod
    def validate_core_type(cls, value):
        return _strict_validate_int_or_raise(value)

    @classmethod
    def validate_core_type_with_choices(cls, value, choices):
        return _strict_validate_int_choices(value, choices)


class PacBioStringChoiceOption(BaseChoiceType):
    OPTION_TYPE_ID = TaskOptionTypes.CHOICE_STR

    @classmethod
    def validate_core_type(cls, value):
        return _strict_validate_string_or_raise(value)

    @classmethod
    def validate_core_type_with_choices(cls, value, choices):
        return _strict_validate_str_choices(value, choices)


class PacBioFloatChoiceOption(BaseChoiceType):
    OPTION_TYPE_ID = TaskOptionTypes.CHOICE_FLOAT

    @classmethod
    def validate_core_type(cls, value):
        return _strict_validate_float_or_raise(value)

    @classmethod
    def validate_core_type_with_choices(cls, value, choices):
        return _strict_validate_float_choices(value, choices)


def _get_exception_name(e):
    if isinstance(e, Exception):
        return e.__class__.__name__
    else:
        return str(e)


def _get_level_name(l):
    if isinstance(l, int):
        return logging.getLevelName(l)
    else:
        return l


class PacBioAlarm:
    """
    Simple container for alarms that need to be passed between components in
    SMRT Link.  This mimics the interface in the instrument control code; note
    that not all fields will necessarily be used by SMRT Link.  The JSON I/O
    format is always a list to support the possibility of multiple alarms
    created by the same task, but we mostly only need to work with one at a
    time.
    """

    def __init__(self,
                 name,
                 message,
                 exception=None,
                 info=None,
                 severity=logging.ERROR,
                 created_at=None,
                 owner=None,
                 id_=None):
        self.message = message
        self.name = name
        self.exception = exception
        self.info = info
        self.severity = _get_level_name(severity)
        self.owner = owner
        self.id = id_ if id_ is not None else str(uuid.uuid4())
        self.created_at = created_at if created_at is not None else datetime.datetime.now()

    @staticmethod
    def from_dict(d):
        return PacBioAlarm(d["name"],
                           d["message"],
                           d["exception"],
                           d["info"],
                           d["severity"],
                           owner=d.get("owner", None),
                           id_=d.get("id", None))

    @property
    def log_level(self):
        return getattr(logging, self.severity)

    def to_dict(self):
        return {
            "exception": _get_exception_name(self.exception),
            "info": self.info,
            "message": self.message,
            "name": self.name,
            "severity": self.severity,
            "owner": self.owner,
            "createdAt": _datetime_to_string(self.created_at),
            "id": str(self.id)
        }

    def to_json(self, file_name):
        with open(file_name, "w") as json_out:
            json_out.write(json.dumps([self.to_dict()],
                                      indent=2,
                                      separators=(',', ': '),
                                      sort_keys=True))
        return self

    @staticmethod
    def from_json(file_name):
        with open(file_name, "r") as json_in:
            return PacBioAlarm.from_dict(json.loads(json_in.read())[0])

    def raise_exception(self):
        raise self.exception(self.message)

    @staticmethod
    def dump_error(file_name,
                   exception,
                   info,
                   message,
                   name,
                   severity):
        return PacBioAlarm(
            name,
            message,
            exception,
            info,
            severity,
            owner="python").to_json(file_name)


class PipelinePreset:

    def __init__(self, options, task_options, pipeline_id,
                 preset_id, name, description):
        self.options = options
        self.task_options = task_options
        self.pipeline_id = pipeline_id
        self.preset_id = preset_id
        self.name = name
        self.description = description

    def __repr__(self):
        _d = dict(k=self.__class__.__name__)  # self.to_dict()
        return "<{k} >".format(**_d)

    def to_dict(self):
        return OrderedDict([
            ("pipelineId", self.pipeline_id),
            ("presetId", self.preset_id),
            ("name", self.name),
            ("description", self.description),
            ("options", dict(self.options)),
            ("taskOptions", dict(self.task_options))])


class EntryPoint(namedtuple("EntryPoint", ["entry_id", "file_type_id", "name", "optional"])):

    @staticmethod
    def from_dict(d):
        return EntryPoint(d["entryId"], d["fileTypeId"], d["name"],
                          d.get("optional", False))

    @property
    def short_name(self):
        return self.file_type_id.split(".")[-1] + " XML"

    @property
    def file_ext(self):
        return FileTypes.ALL()[self.file_type_id].ext
