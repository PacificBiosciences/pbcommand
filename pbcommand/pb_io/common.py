import logging
import json
import sys

from pbcommand.models import PipelineChunk

log = logging.getLogger(__name__)


def write_pipeline_chunks(chunks, output_json_file, comment):

    _d = dict(nchunks=len(chunks), _version="0.1.0",
              chunks=[c.to_dict() for c in chunks])

    if comment is not None:
        _d['_comment'] = comment

    with open(output_json_file, 'w') as f:
        f.write(json.dumps(_d, indent=4))

    log.debug("Write {n} chunks to {o}".format(n=len(chunks), o=output_json_file))


def load_pipeline_chunks_from_json(path):
    """Returns a list of Pipeline Chunks


    :rtype: list[PipelineChunk]
    """

    try:
        with open(path, 'r') as f:
            d = json.loads(f.read())

        chunks = []
        for cs in d['chunks']:
            chunk_id = cs['chunk_id']
            chunk_datum = cs['chunk']
            c = PipelineChunk(chunk_id, **chunk_datum)
            chunks.append(c)
        return chunks
    except Exception:
        msg = "Unable to load pipeline chunks from {f}".format(f=path)
        sys.stderr.write(msg + "\n")
        raise
