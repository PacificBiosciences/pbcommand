"""Example of Gather TC to gather several $chunk.fasta_id in chunk.json file.


There's a bit of code here that is copied from pbsmrtpipe.tools.chunk_utils.
Martin will eventually refactor this into pbcore.
"""
import logging
import sys
import warnings

import functools

from pbcommand.cli import pbparser_runner
from pbcommand.models import get_gather_pbparser, FileTypes
from pbcommand.pb_io import load_pipeline_chunks_from_json
from pbcommand.utils import setup_log

from .dev_scatter_fasta_app import Constants

log = logging.getLogger(__name__)

TOOL_ID = "pbcommand.tasks.dev_gather_fasta"
__version__ = '0.1.0'


try:
    from pbcore.io import FastaWriter, FastaReader
except ImportError:
    warnings.warn("Example apps require pbcore. Install from https://github.com/PacificBiosciences/pbcore")


def __gather_fastx(fastx_reader, fastx_writer, fastx_files, output_file):
    # this will work for any Pbcore Reader, Writer classes
    n = 0
    with fastx_writer(output_file) as writer:
        for fastx_file in fastx_files:
            with fastx_reader(fastx_file) as reader:
                for record in reader:
                    n += 1
                    writer.writeRecord(record)

    log.info("Completed gathering {n} files (with {x} records) to {f}".format(n=len(fastx_files), f=output_file, x=n))
    return 0

gather_fasta = functools.partial(__gather_fastx, FastaReader, FastaWriter)


def _get_datum_from_chunks_by_chunk_key(chunks, chunk_key):
    datum = []
    for chunk in chunks:
        if chunk_key in chunk.chunk_keys:
            value = chunk.chunk_d[chunk_key]
            datum.append(value)
        else:
            raise KeyError("Unable to find chunk key '{i}' in {p}".format(i=chunk_key, p=chunk))

    return datum


def __args_gather_runner(func, chunk_json, output_file, chunk_key):
    chunks = load_pipeline_chunks_from_json(chunk_json)

    # Allow looseness
    if not chunk_key.startswith('$chunk.'):
        chunk_key = '$chunk.' + chunk_key
        log.warn("Prepending chunk key with '$chunk.' to '{c}'".format(c=chunk_key))
    else:
        chunk_key = chunk_key

    fastx_files = _get_datum_from_chunks_by_chunk_key(chunks, chunk_key)
    _ = func(fastx_files, output_file)
    return 0


def run_main(chunked_json, output_fasta, chunk_key):
    """Create a Chunk.json file with nchunks <= max_nchunks

    Not clear on the nchunks vs max_nchunks.
    """
    return __args_gather_runner(gather_fasta, chunked_json, output_fasta, chunk_key)


def get_parser():

    driver = "python -m pbcommand.cli.examples.dev_scatter_fasta_app --resolved-tool-contract "
    desc = "Gather a fasta resources in a Chunk.json file"
    # chunk keys that will be written to the file
    chunk_key = "$chunk.fasta_id"
    p = get_gather_pbparser(TOOL_ID, __version__, "Fasta Chunk Gather",
                            desc, driver, is_distributed=False)
    p.add_input_file_type(FileTypes.CHUNK, "chunk_json", "Chunk JSON", "Chunked Fasta JSON Out")
    p.add_output_file_type(FileTypes.FASTA, "output", "Chunk JSON", "Output Fasta", "gathered.fasta")
    return p


def args_runner(args):
    return run_main(args.chunk_json, args.output, Constants.FA_CHUNK_KEY)


def rtc_runner(rtc):
    return run_main(rtc.task.input_files[0],
                    rtc.task.output_files[0],
                    Constants.FA_CHUNK_KEY)


def main(argv=sys.argv):
    return pbparser_runner(argv[1:],
                           get_parser(),
                           args_runner,
                           rtc_runner,
                           log,
                           setup_log)


if __name__ == '__main__':
    sys.exit(main())
