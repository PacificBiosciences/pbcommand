pbcommand High Level Overview
=============================

[Full Docs](http://pbcommand.readthedocs.org/en/latest/)

Note the APIs are still in flux. WIP.

[![Circle CI](https://circleci.com/gh/PacificBiosciences/pbcommand.svg?style=svg)](https://circleci.com/gh/PacificBiosciences/pbcommand)

PacBio library for common utils, models, and tools to interface with pbsmrtpipe workflow engine.

To integrate with the pbsmrtpipe workflow engine you must to be able to generate a **Tool Contract** and to be able to run from a **Resolved Tool Contract**.

A **Tool Contract** contains the metadata of the exe, such as the file types of inputs, outputs and options.

A hello world example:

```javascript
{
    "version": "0.2.0", 
    "driver": {
        "exe": "python -m pbcommand.cli.example.dev_app --resolved-tool-contract ", 
        "env": {}
    }, 
    "tool_contract_id": "pbcommand.tools.dev_app", 
    "tool_contract": {
        "input_types": [
            {
                "description": "PacBio Spec'ed fasta file", 
                "title": "Fasta File", 
                "id": "fasta_in", 
                "file_type_id": "PacBio.FileTypes.Fasta"
            }
        ], 
        "task_type": "pbsmrtpipe.task_types.standard", 
        "resource_types": [], 
        "nproc": 1, 
        "schema_options": [
            {
                "$schema": "http://json-schema.org/draft-04/schema#", 
                "required": [
                    "pbcommand.task_options.dev_read_length"
                ], 
                "type": "object", 
                "properties": {
                    "pbcommand.task_options.dev_read_length": {
                        "default": 25, 
                        "type": "integer", 
                        "description": "Min Sequence Length filter", 
                        "title": "Length filter"
                    }
                }, 
                "title": "JSON Schema for pbcommand.task_options.dev_read_length"
            }
        ], 
        "output_types": [
            {
                "title": "Filtered Fasta file", 
                "description": "Filtered Fasta file", 
                "default_name": "filter.fasta", 
                "id": "fasta_out", 
                "file_type_id": "PacBio.FileTypes.Fasta"
            }
        ], 
        "_comment": "Created by v0.1.0 at 2015-07-21T22:36:09.466032"
    }
}
```

A **Resolved Tool Contract** is a resolved **Tool Contract** where types and resources and resolved to specific entites. 

```javascript
{
  "_comment": "Example of a Resolved Tool Contract.",
  "tool_contract": {
    "tool_contract_id": "pbsmrtpipe.tools.dev_app",
    "task_type": "pbsmrtpipe.task_types.standard",
    "input_files": ["/tmp/file.fasta"],
    "output_files": ["/tmp/file2.filtered.fasta"],
    "options": {"pbsmrtpipe.options.dev_read_length": 50},
    "nproc": 1,
    "resources": [
      ["$tmpdir", "/tmp/tmpdir"],
      ["$logfile", "/tmp/task-dir/file.log"]]
  },
  "driver": {
    "_comment": "This is the driver exe. The workflow will call ${exe} config.json",
    "exe": "python -m pbcommand.cli.examples.dev_app --resolved-tool-contract",
    "env": {}
  }
}
```

A driver is the commandline interface that the workflow engine will call.

The driver will be called with "${exe} /path/to/resolved_tool_contract.json"


Creating a Commandline Tool using tool contract
-----------------------------------------------

Three Steps
- define Parser
- add running from argparse and running from Resolved ToolContract funcs to call your main
- add call to driver

Import or define your main function.

```python
def run_my_main(fasta_in, fasta_out, min_length):
    # do stuff. Main should return an int exit code
    return 0
```

Define a function that will add inputs, outputs and options to your parser.

```python
from pbcommand.models import FileTypes

def add_args_and_options(p):
    # FileType, label, name, description
    p.add_input_file_type(FileTypes.FASTA, "fasta_in", "Fasta File", "PacBio Spec'ed fasta file")
    # File Type, label, name, description, default file name
    p.add_output_file_type(FileTypes.FASTA, "fasta_out", "Filtered Fasta file", "Filtered Fasta file", "filter.fasta")
    # Option id, label, default value, name, description
    # for the argparse, the read-length will be translated to --read-length and (accessible via args.read_length)
    p.add_int("pbcommand.task_options.dev_read_length", "read-length", 25, "Length filter", "Min Sequence Length filter")
    return p
```

Define Parser

```python
from pbcommand.models import TaskTypes, ResourceTypes, SymbolTypes
def get_contract_parser():
    # Number of processors to use, can also be SymbolTypes.MAX_NPROC
    nproc = 1
    # Log file, tmp dir, tmp file. See ResourceTypes in models, ResourceTypes.TMP_DIR
    resource_types = ()
    # Commandline exe to call "{exe}" /path/to/resolved-tool-contract.json
    driver_exe = "python -m pbcommand.cli.example.dev_app --resolved-tool-contract "
    desc = "Dev app for Testing that supports emitting tool contracts"
    task_type = TaskTypes.LOCAL 
    # TaskTypes.DISTRIBUTED if you want your task to be submitted to the cluster manager (e.g., SGE) if 
    # one is provided to the workflow engine.
    p = get_pbparser(TOOL_ID, __version__, desc, driver_exe, task_type, nproc, resource_types)
    add_args_and_options(p)
    return p

```
        

Define a Wrapping layer to call your main from both the tool contract and raw argparse IO layer

```python
def _args_runner(args):
    # this is the args from parser.parse_args()
    # the properties of args are defined as "labels" in the add_args_and_options func.
    return run_my_main(args.fasta_in, fasta_out, args.read_length)
    
def _resolved_tool_contract_runner(resolved_tool_contract):
    rtc = resolved_tool_contract
    # all options are referenced by globally namespaced id. This allows tools to use other tools options
    # e.g., pbalign to use blasr defined options.
    return run_my_main(rtc.inputs[0], rtc.outputs[0], rtc.options["pbcommand.task_options.dev_read_length"])
```
    
    
    
    
Add running layer

```python
import sys
import logging
import pbcommand.utils setup_log
from pbcommand.cli import pbparser_runner

log = logging.getLogger(__name__)

def main(argv=sys.argv):
    # New interface that supports running resolved tool contracts
    log.info("Starting {f} version {v} pbcommand example dev app".format(f=__file__, v=__version__))
    p = get_contract_parser()
    return pbparser_runner(argv[1:], p,
                                               _args_runner, # argparse runner func
                                               _resolved_tool_contract_runner, # tool contract runner func
                                               log, # log instance
                                               setup_log # setup log func
                                               )
if __name__ == '__main__':
    sys.exit(main())
```

Now you can run your tool via the argparse standard interface as well as emitting a **Tool Contract** to stdout from the commandline interface.

```sh
> python -m 'pbcommand.cli.examples.dev_app' --emit-tool-contract
```

And you can run the tool from a **Resolved Tool Contract**

```sh
> python -m pbcommand.cli.example.dev_app --resolved-tool-contract /path/to/resolved_contract.json
```

See the dev apps in ["pbcommand.cli.examples"](https://github.com/PacificBiosciences/pbcommand/blob/master/pbcommand/cli/examples/dev_app.py) for a complete application (They require pbcore to be installed).

An example of **help** is shown below.

```sh
(pbcommand_test)pbcommand $> python -m 'pbcommand.cli.examples.dev_app' --help
usage: dev_app.py [-h] [-v] [--versions] [--emit-tool-contract]
                  [--resolved-tool-contract RESOLVED_TOOL_CONTRACT]
                  [--log-level LOG_LEVEL] [--debug]
                  [--read-length READ_LENGTH]
                  fasta_in fasta_out

Dev app for Testing that supports emitting tool contracts

positional arguments:
  fasta_in              PacBio Spec'ed fasta file
  fasta_out             Filtered Fasta file

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  --versions            Show versions of individual components (default: None)
  --emit-tool-contract  Emit Tool Contract to stdout (default: False)
  --resolved-tool-contract RESOLVED_TOOL_CONTRACT
                        Run Tool directly from a PacBio Resolved tool contract
                        (default: None)
  --log-level LOG_LEVEL
                        Set log level (default: 10)
  --debug               Debug to stdout (default: False)
  --read-length READ_LENGTH
                        Min Sequence Length filter (default: 25)
```

