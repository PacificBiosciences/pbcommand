pbcommand High Level Overview
=============================

PacBio library for common utils, models, and tools to interface with pbsmrtpipe workflow engine.

To integrate with the pbsmrtpipe workflow engine you must to be able to generate a **Tool Contract** and to be able to run from a **Resolved Tool Contract**.

A **Tool Contract** contains the metadata of the exe, such as the file types of inputs, outputs and options.

A hello world example:

```javascript
{
  "_comment": "Example Tool Contract Interface using Pbsystem",
  "driver": {
    "_comment": "The execution layer will call '${exe} manifest.json' as the to_cmd",
    "exe": "python -m pbsystem.common.cmdline.dev_app --resolved-tool-contract ",
    "env": {}
  },
  "tool_contract": {
    "tool_contract_id": "pbsystem.common.cmdline.dev_app",
    "name": "Pbsystem Sample Dev Task",
    "tool_type": "pbsmrtpipe.constants.local_task",
    "version": "0.2.1",
    "description": "Pbsystem that does awesome things.",
    "input_types": ["PacBio.FileTypes.Fasta"],
    "output_types": ["PacBio.FileTypes.Fasta"],
    "schema_options": {},
    "nproc": 1,
    "resource_types": ["$tmpdir", "$logfile"]
  }
}
```

A **Resolved Tool Contract** is a resolved **Tool Contract** where types and resources and resolved to specific entites. 

```javascript
{
  "_comment": "Example of a Resolved Tool Contract.",
  "tool_contract": {
    "tool_contract_id": "pbsystem.tools.dev_app",
    "tool_type": "pbsmrtpipe.constants.local_task",
    "input_files": ["/tmp/file.dataset.txt"],
    "output_files": ["/tmp/output.txt"],
    "options": {},
    "nproc": 3,
    "resources": [
      ["$tmpdir", "/tmp/tmpdir"],
      ["$logfile", "/tmp/task-dir/file.log"]]
  },
  "driver": {
    "_comment": "This is the driver exe. The workflow will call ${exe} config.json",
    "exe": "python -m pbsystem.common.cmdline.dev_app --resolved-tool-contract ",
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
- add running from argparse and running from ToolContract funcs to call your main
- call driver

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
    p = get_default_contract_parser(TOOL_ID, __version__, desc, driver_exe, task_type, nproc, resource_types)
    add_args_and_options(p)
    return p

```
        

Define a Wrapping layer to call your main from both the tool contract and raw argparse IO layer

```python
def _args_runner(args):
    return run_my_main(args.fasta_in, fasta_out, args.read_length)
    
def _resolved_tool_contract_runner(resolved_tool_contract):
    rtc = resolved_tool_contract
    return run_my_main(rtc.inputs[0], rtc.outputs[0], rtc.options["pbcommand.task_options.dev_read_length"])
```
    
    
    
    
Add running layer

```python
import sys
import logging
import pbcommand.utils setup_log
from pbcommand.cli import pacbio_args_or_contract_runner_emit

log = logging.getLogger(__name__)

def main(argv=sys.argv):
    # New interface that supports running resolved tool contracts
    log.info("Starting {f} version {v} pbcommand example dev app".format(f=__file__, v=__version__))
    p = get_contract_parser()
    return pacbio_args_or_contract_runner_emit(argv[1:], p,
                                               _args_runner, # argparse runner func
                                               _resolved_tool_contract_runner, # tool contract runner func
                                               log, # log instance
                                               setup_log # setup log func
                                               )
if __name__ == '__main__':
    sys.exit(main())
```

Now you can emit a **Tool Contract** to stdout from the commandline interface.

```sh
> my-tool --emit-tool-contract
```

And you can run the tool from a **Resolved Tool Contract**

```sh
> my-tool --resolved-tool-contract /path/to/resolved_contract.json
```

See the dev apps in ["pbcommand.cli.examples"](https://github.com/PacificBiosciences/pbcommand/blob/master/pbcommand/cli/examples/dev_app.py) for a complete application (They require pbcore to be installed).

```sh
(pbcommand_test)pbcommand $> python -m pbcommand.cli.examples.dev_app --help
usage: dev_app.py [-h] [-v] [--emit-tool-contract]
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

