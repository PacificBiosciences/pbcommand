import unittest
import logging
from pbcommand.testkit.base_utils import get_temp_dir

log = logging.getLogger(__name__)

from pbcommand.models import PipelineChunk
from pbcommand.pb_io import load_pipeline_chunks_from_json, write_pipeline_chunks

from base_utils import get_temp_file


class TestWriteChunk(unittest.TestCase):

    def test_write_chunks(self):

        def f(i):
            return {"{c}movie_fofn_id".format(c=PipelineChunk.CHUNK_KEY_PREFIX): "/path/to_movie-{i}.fofn".format(i=i),
                    "{c}region_fofn_id".format(c=PipelineChunk.CHUNK_KEY_PREFIX): "/path/rgn_{i}.fofn".format(i=i)}

        to_i = lambda i: "chunk-id-{i}".format(i=i)
        to_p = lambda i: PipelineChunk(to_i(i), **f(i))

        nchunks = 5
        pipeline_chunks = [to_p(i) for i in xrange(nchunks)]
        log.debug(pipeline_chunks)
        tmp_dir = get_temp_dir("pipeline-chunks")
        tmp_name = get_temp_file("_chunk.json", tmp_dir)

        write_pipeline_chunks(pipeline_chunks, tmp_name, "Example chunk file")

        pchunks = load_pipeline_chunks_from_json(tmp_name)
        self.assertEquals(len(pchunks), nchunks)