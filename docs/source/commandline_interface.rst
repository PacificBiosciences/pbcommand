Common Commandline Interface
============================


Motivation And High Level Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Provide a common interface for executables to expose options
- Provide a common interface for executables to be called
- Provide a common interface for exposing metadata of tool, such as memory usage, cpu usage, required temp files

Benefits
~~~~~~~~

- A consistent concrete common interface for shelling out to an executable
- task options have a consistent model for validation
- task version is supported
- A principled model for wrapping tools. For example, pbalign would "inherit" blasr options and extend, or wrap them.
- Once a manifest has been defined and registered to pbsmrtpipe, the task/manifest can be referenced in pipelines with no additional work


Terms
~~~~~

- 'Tool Contract' is a single file that exposing the exe interface. It
  contains metadata about the task, such as input and output file
  types, nproc.
- 'Resolved Tool Contract' is a single file that contains the resolved values in the manifest
- 'Driver' is the general interface for calling a commandline exe. This can be called from the commandline or directly as an API call (via any language which supports the manifest interface).

Hello World Dev Example
~~~~~~~~~~~~~~~~~~~~~~~

Tool Contract example for an exe, 'python -m pbcommand.cli.example.dev_app` with tool contract id `pbcommand.tasks.dev_app`.


.. literalinclude:: ../../tests/data/tool-contracts/pbcommand.tasks.dev_app_tool_contract.json
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

pbcommand provides a API to create a tool contract and an argparse instance from a single interface. This facilitates a single point of defining options and keeps the standard commandline entry point and the tool contract to be in sync.

This also allows your tool to emit the tool contract to stdout using "--emit-tool-contract" **and** the tool to be run from a **Resolved Tool Contract** using the "--resolved-tool-contract /path/to/resolved-tool-contract.json" commandline argument **while** also supporting the python standards commandline interface via argparse.

Complete App shown below.


.. literalinclude:: ../../pbcommand/cli/examples/dev_app.py
    :language: python

.. note:: Options must be prefixed with {pbcommand}.task_options.{option_id} format.

Details and Example of a Resolved Tool Contract
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Language agnostic JSON format to encode the resolved values
- input, outputs file types are resolved to file paths
- nproc and other resources are resolved
- IO layers to convert between JSON and python using `load_resolved_tool_contract_from` in `pbcommand.pb_io`

Example Resolved Tool Contract:

.. literalinclude:: ../../tests/data/resolved-tool-contracts/dev_example_resolved_tool_contract.json
    :language: javascript


Testing Tool Contracts
~~~~~~~~~~~~~~~~~~~~~~

There is a thin test framework in `pbcommand.testkit` to help test tool contracts from within nose.

The `PbTestApp` base class will provide the core validation of the outputs as well as handled the creation of the resolved tool contract.

Output Validation assertions

- validates Output files exist
- validates resolved task options
- validates resolved value of is distributed
- validates resolved value of nproc

Example:

.. literalinclude:: ../../tests/test_e2e_example_apps.py
    :language: python


Tips
~~~~

A dev tool within pbcommand can help convert Tool Contract JSON files to Resolved Tool Contract for testing purposes.


.. argparse::
   :module: pbcommand.interactive_resolver
   :func: get_parser
   :prog: python -m pbcommand.interactive_resolver


.. note::  This tool has dependency on `prompt_kit` and can be installed via pip.