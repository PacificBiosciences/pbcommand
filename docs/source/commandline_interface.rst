Common Commandline Interface
============================


Motivation And High Level Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Provide a common interface for executables to expose options
- Provide a common interface for executables to be called
- Provide a common interface for exposing metadata of tool, such as memory usage, cpu usage, required temp files

Benefits
~~~~~~~~

- A consistent concrete common interface for shelling out to executables
- task options have a consistent model for validation
- task versioning is supported
- A principled model for wrapping tools. For example, pbalign would "inherit" blasr options and extend, or wrap them.
- Once a manifest has been defined and registered to pbsmrtpipe, the task/manifest can be referenced in pipelines with no additional work


Terms
~~~~~

- 'Tool Contract' is a single file that exposing the exe interface. It
  contains metadata about the task, such as input and output file
  types, nproc.
- 'Resolved Tool Contract' is a single file that contains the resolved values in the manifest
- 'Driver' is the general interface for calling a commandline exe. This can be called from the commandline or directly as an API call (via any language which supports the manifest interface).

Hello World Example
~~~~~~~~~~~~~~~~~~~

Tool Contract file for 'my-exe'


.. literalinclude:: ../tests/data/dev_example_tool_contract.json
    :language: javascript


Details of Tool Contract
~~~~~~~~~~~~~~~~~~~~~~~~

- Tool Contract id which can be referenced globally (e.g., within a pipeline template)
- Input File types have file type id, id that can be referenced within the driver, and a brief description
- Output File types have a file type id and a default output file name
- number of processors is defined by $nproc. "\$" prefixed values are symbols that have well defined semantic meaning
- Temp files and Log files are defined using "$" symbols are can have multiple items
- the exe options are exposed via jsonschema standard. Each option has an id and maps to a single schema definition. Each option must have a default value.
- the exe section of the "driver" is the commandline interface that will be called as a positional arguement (e.g., "my-exe resolved-manifest.json")
- task type describes if the task should be submitted to the cluster resources


Note. A single driver can reference many manifests. For example "pbreports" would have a single driver exe. From the "task_manifest_id", the driver would dispatch to the correct function call

Programmatically defining a Parser to Emit a Tool Contract
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

pbsystem provides a API to create a tool contract and an argparse instance from a single interface. This facilitates a single point of defining options and keeps the standard commandline entry point and the tool contract to be in sync. This also allows your tool to emit the tool contract to stdout using "--emit-tool-contract" and to be run from a **Resolved Tool Contract** using the "--resolved-tool-contract /path/to/resolved-tool-contract.json" commandline argument.

Complete App shown below.


.. literalinclude:: ../pbsystem/common/cmdline/examples/dev_app.py
    :language: python

.. note:: Options must be prefixed with {pbsystem}.task_options.{option_id} format.

Details of Resolved Tool Contract
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- input, outputs file types are resolved to file paths
- nproc and other resources are resolved


.. literalinclude:: ../tests/data/dev_example_resolved_tool_contract.json
    :language: javascript


Library usage
~~~~~~~~~~~~~

(language API example)


Example of using a manifest in an tool, such as mapping status report.

.. code-block:: python

    # your application was called via "pbreports resolved-manifest.json"

    # load resolved manifest from
    r = load_manifest_json(sys.argv[1])

    # general call to mapping stats report main
    # mapping_stats_main("/path/to/align.dataset.xml", "/path/to/reference.dataset.xml", "/path/to/output.json", my_option=1235)
    exit_code = mapping_stats_main(r.input_files[0], r.input_files[1], r.output_files[0], r.opts["pbreports.options.my_option"])


Example to resolving the Tool Contract
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The resolver must have assigned values for max nproc, root temp dir,
output dir. The output dir can be used to assign the output paths of
the output files.

.. code-block:: python

    # simple python example, the scala or C++ API would be similar

    # load tool contract that is registered to your python package
    tool_contract = ToolContractRegistry.get("pbsmrtpipe.tasks.dev_static_task")

    max_nproc = 3
    tmp_dir = "/tmp/my-tmp"
    output_dir = os.getcwd()

    # The resolver assigns the output file names, nproc, tmp files, tmp dirs, etc...
    resolver = SimpleToolContractResolver(tool_contract, output_dir, max_nproc, tmp_dir)

    input_files = ("/path/to/file.csv", "/path/to/dataset.subreads.xml")

    opts = {"pbsmrtipe.task_options.my_option": 1234}
    is_valid = resolver.validate_opts(opts)

    # Create a resolved ToolContract
    resolved_tool_contract = resolver.resolve(input_files, opts=opts)

    # The driver will run the tool, validate output files exist and
    # cleanup any temp files/resources.
    result = run_tool_contract_driver(resolved_tool_contract, cleanup=False)

    print result.exit_code
    print result.error_message
    print result.host_name
    print result.run_time

    # sugar to persist results
    result.write_json("output-results.json")


