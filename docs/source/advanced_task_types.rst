Advanced Task/ToolContract Types
================================


To enable pipeline scaling, "Chunking" of files two new Tool Contract types extend the base Tool Contract data model.



Scattering/Chunking Tool Contract
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Tasks/ToolContract that take a any file type(s) and emit a **single** scatter.chunk.json file.


At a high level, the Scatter Tool Contract data model extends the core Tool Contract model and adds two fields, `chunk_keys` and `nchunks`.

- `chunk_keys` is the expected key(s) that will be written to the `PipelineChunk` data model (defined below)
- `nchunks` mirrors the `nproc` model of using a symbol `$max_nchunks` or an int to define the absolute upper bound on the number of chunks that should be created. If this value is exceeded, the pipeline engine will immediately fail the execution.


Example Tool Contract

.. literalinclude:: ../../tests/data/tool-contracts/dev_scatter_fasta_app_tool_contract.json
    :language: javascript



PipelineChunk Data Model
~~~~~~~~~~~~~~~~~~~~~~~~


The `PipelineChunk` data model is defined in `pbcommand.models` and the companion IO layers (`load_pipeline_chunks_from_json` and `write_pipeline_chunks` are in `pbcommand.pb_io`.

Each input file **must** be mapped to a `chunk_key` that can then be mapped to the input of the original `unchunked` task.

For example, if there's a single input file (e.g., FileTypes.FASTA), then the Scatter ToolContract should define a `chunk_key` of "fasta_id". `chunk_key`(s) that do NOT start with `$chunk.` will considered to be extra metadata that will be passed through. This is useful for adding chunk specific metadata, such as the number of contigs or average contig length.

Minimal example of reading and writing `PipelineChunk(s)` data model.

.. ipython::


    In [1]: from pbcommand.models import PipelineChunk


    In [5]: c0 = PipelineChunk("scattered-fasta_0", **{"$chunk.fasta_id":"/path/to/chunk-0.fasta"})

    In [6]: c1 = PipelineChunk("scattered-fasta_1", **{"$chunk.fasta_id":"/path/to/chunk-1.fasta"})

    In [7]: chunks = [c0, c1]

    In [8]: from pbcommand.pb_io import write_pipeline_chunks

    In [10]: write_pipeline_chunks(chunks, "test-scatter.chunk.json", "Test comment")

    In [11]: from pbcommand.pb_io import load_pipeline_chunks_from_json

    In [12]: load_pipeline_chunks_from_json("test-scatter.chunk.json")
    Out[12]:
    [<PipelineChunk id='scattered-fasta_0' chunk keys=$chunk.fasta_id >,
     <PipelineChunk id='scattered-fasta_1' chunk keys=$chunk.fasta_id >]





Defining a Scatter Tool Contract
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Currently, python is the only language that is supported for writing CHUNK JSON files.

The python Scatter tool contract API follows similar to base Tool Contract API,


Example of Scattering/Chunking a Fasta file. The notable points are adding the required `chunk_keys` and `nchunks` to the scattering specific pbparser.


.. literalinclude:: ../../pbcommand/cli/examples/dev_scatter_fasta_app.py
    :language: python



Gather ToolContract
~~~~~~~~~~~~~~~~~~~


A Gather Tool Contract takes a **single** CHUNK Json file type as input and emits a **single** output file of any type.



Example:

.. literalinclude:: ../../tests/data/tool-contracts/dev_gather_fasta_app_tool_contract.json
    :language: javascript


The Gather task doesn't extend the base ToolContract and add new properties. However, it will restrict the the input type to `FileTypes.CHUNK` and the output type signature **must only be one file type**.


Example Gather Tool:

.. literalinclude:: ../../pbcommand/cli/examples/dev_gather_fasta_app.py
    :language: python


For Gather'ing a task that has a multiple N outputs, N gather tasks must be defined.

See the pbsmrtpipe_ docs for details of constructing a chunked pipeline.


More examples of scatter/chunking and gather tasks are in pbcoretools_.

.. _pbsmrtpipe: http://pbsmrtpipe.readthedocs.io

.. _pbcoretools: https://github.com/PacificBiosciences/pbcoretools/tree/master/pbcoretools/tasks