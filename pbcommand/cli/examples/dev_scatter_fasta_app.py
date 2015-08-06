"""Example of Generating a Chunk.json file that 'scatters' a pair of fasta files


In the example, the first fasta file is chunked, while the path to the second
fasta file is passed directly.

It generates a fasta_1_id and fasta_2_id chunk keys,

There's a bit of code here that is copied from pbsmrtpipe.tools.chunk_utils.

Martin will eventually refactor this into pbcore.
"""
import os
import logging
import sys
import warnings
import math
import datetime

from pbcommand.cli import pbparser_runner
from pbcommand.models import get_scatter_pbparser, FileTypes, PipelineChunk
from pbcommand.pb_io import write_pipeline_chunks
from pbcommand.utils import setup_log

log = logging.getLogger(__name__)

TOOL_ID = "pbcommand.tasks.dev_scatter_fasta"
__version__ = '0.1.0'


try:
    from pbcore.io import FastaWriter, FastaReader
except ImportError:
    warnings.warn("Example apps require pbcore. Install from https://github.com/PacificBiosciences/pbcore")


class Constants(object):
    NCHUNKS_OPT = "pbcommand.task_options.dev_scatter_fa_nchunks"
    FA_CHUNK_KEY = "$chunk.fasta_id"


def __get_nrecords_from_reader(reader):
    n = 0
    for _ in reader:
        n += 1
    return n


def write_fasta_records(fastax_writer_klass, records, file_name):

    n = 0
    with fastax_writer_klass(file_name) as w:
        for record in records:
            w.writeRecord(record)
            n += 1

    log.debug("Completed writing {n} fasta records".format(n=n))


def __to_chunked_fastx_files(fastx_reader_klass, fastax_writer_klass, chunk_key, fastx_path, max_total_nchunks, dir_name, base_name, ext):
    """Convert a Fasta/Fasta file to a chunked list of files"""

    # grab the number of records so we can chunk it
    with fastx_reader_klass(fastx_path) as f:
        nrecords = __get_nrecords_from_reader(f)

    max_total_nchunks = min(nrecords, max_total_nchunks)

    n = int(math.ceil(float(nrecords)) / max_total_nchunks)

    nchunks = 0
    with fastx_reader_klass(fastx_path) as r:
        it = iter(r)
        for i in xrange(max_total_nchunks):
            records = []

            chunk_id = "_".join([base_name, str(nchunks)])
            chunk_name = ".".join([chunk_id, ext])
            nchunks += 1
            fasta_chunk_path = os.path.join(dir_name, chunk_name)

            if i != max_total_nchunks:
                for _ in xrange(n):
                    records.append(it.next())
            else:
                for x in it:
                    records.append(x)

            write_fasta_records(fastax_writer_klass, records, fasta_chunk_path)
            total_bases = sum(len(r.sequence) for r in records)
            d = dict(total_bases=total_bases, nrecords=len(records))
            d[chunk_key] = os.path.abspath(fasta_chunk_path)
            c = PipelineChunk(chunk_id, **d)
            yield c


def to_chunked_fasta_files(fasta_path, max_total_nchunks, dir_name, chunk_key, base_name, ext):
    return __to_chunked_fastx_files(FastaReader, FastaWriter, chunk_key, fasta_path, max_total_nchunks, dir_name, base_name, ext)


def write_chunks_to_json(chunks, chunk_file):
    log.debug("Wrote {n} chunks to {f}.".format(n=len(chunks), f=chunk_file))
    write_pipeline_chunks(chunks, chunk_file, "Chunks written at {d}".format(d=datetime.datetime.now()))
    return 0


def _write_fasta_chunks_to_file(to_chunk_fastx_file_func, chunk_file, fastx_path, max_total_chunks, dir_name, chunk_key, chunk_base_name, chunk_ext):
    chunks = list(to_chunk_fastx_file_func(fastx_path, max_total_chunks, dir_name, chunk_key, chunk_base_name, chunk_ext))
    write_chunks_to_json(chunks, chunk_file)
    return 0


def write_fasta_chunks_to_file(chunk_file, fasta_path, max_total_chunks, dir_name, chunk_key, chunk_base_name, chunk_ext):
    return _write_fasta_chunks_to_file(to_chunked_fasta_files, chunk_file, fasta_path, max_total_chunks, dir_name, chunk_key, chunk_base_name, chunk_ext)


def run_main(fasta_file, chunk_output_json, chunk_key, max_nchunks, nchunks=None, chunk_base_name="fasta"):
    """Create a Chunk.json file with nchunks <= max_nchunks

    Not clear on the nchunks vs max_nchunks.
    """
    output_dir = os.path.dirname(chunk_output_json)
    return write_fasta_chunks_to_file(chunk_output_json, fasta_file, max_nchunks, output_dir, chunk_key, chunk_base_name, "fasta")


def get_parser():

    driver = "python -m pbcommand.cli.examples.dev_scatter_fasta_app --resolved-tool-contract "
    desc = "Scatter a single fasta file to create chunk.json file"
    # chunk keys that will be written to the file
    chunk_keys = ("$chunk.fasta_id", )
    p = get_scatter_pbparser(TOOL_ID, __version__, "Fasta Scatter",
                             desc, driver, chunk_keys, is_distributed=False)
    p.add_input_file_type(FileTypes.FASTA, "fasta_in", "Fasta In", "Fasta file to scatter")
    p.add_output_file_type(FileTypes.CHUNK, "cjson", "Chunk JSON", "Scattered/Chunked Fasta Chunk.json", "fasta.chunks.json")
    p.add_int("pbcommand.task_options.dev_scatter_fa_nchunks", "nchunks", 10, "Number of chunks",
              "Suggested number of chunks. May be overridden by $max_nchunks")
    return p


def args_runner(args):
    return run_main(args.fasta_in, args.cjson, Constants.FA_CHUNK_KEY, args.nchunks)


def rtc_runner(rtc):
    return run_main(rtc.task.input_files[0],
                    rtc.task.output_files[0],
                    Constants.FA_CHUNK_KEY,
                    rtc.task.options[Constants.NCHUNKS_OPT])


def main(argv=sys.argv):
    return pbparser_runner(argv[1:],
                           get_parser(),
                           args_runner,
                           rtc_runner,
                           log,
                           setup_log)


if __name__ == '__main__':
    sys.exit(main())
