"""Example of Generating a Chunk.json file that 'scatters' a pair of fasta files


In the example, the first fasta file is chunked, while the path to the second
fasta file is passed directly.

It generates a fasta_1_id and fasta_2_id chunk keys,

"""
import logging

from pbcommand.models import PipelineChunk, get_scatter_pbparser

log = logging.getLogger(__name__)

TOOL_ID = "pbcommand.tasks.dev_scatter_fasta"
__version__ = '0.1.0'


def run_main(fasta_file, chunk_output_json, max_nchunks, nchunks=None):
    """Create a Chunk.json file with nchunks <= max_nchunks

    Not clear on the nchunks vs max_nchunks.
    """
    return 0


def get_parser():

    driver = "python -m pbcommand.cli.examples.dev_scatter_fasta "
    p = get_scatter_pbparser(TOOL_ID, __version__, "Fasta Scatter", "Scatter a single fasta file")