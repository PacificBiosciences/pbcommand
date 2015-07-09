import pprint

from pbcommand.models.common import (FileTypes, ResourceTypes, SymbolTypes, TaskTypes)
from pbcommand.models.parser import get_default_contract_parser
from pbcommand.models.tool_contract import ToolDriver


def _example_options(p):
    p.add_input_file_type(FileTypes.BAM, "ubam", "Unaligned BAM", "A General description of BAM")
    p.add_input_file_type(FileTypes.DS_REF, "ref", "Reference", "Reference Dataset XML")
    p.add_int("mytool.task_options.myoption", "myopt", 7, "My Option", "My Option which does this and that")
    p.add_str("mytool.task_options.myopt2", "mylabel", "Stuff", "My Option name", "My Option2 description")
    p.add_output_file_type(FileTypes.REPORT, "rpt", "Json Report", "Mapping Stats Report Task", "mapping-stats.report.json")
    return p


def example_01():
    driver = ToolDriver("my-exe --config")
    resource_types = (ResourceTypes.TMP_DIR, ResourceTypes.LOG_FILE)
    p = get_default_contract_parser("pbcommand.tools.example", "0.1.2", "My Description", driver, TaskTypes.DISTRIBUTED, SymbolTypes.MAX_NPROC, resource_types)
    return _example_options(p)


def example_02():
    p = example_01()

    print "Generated Manifest"
    pprint.pprint(p.parsers[1].to_tool_contract())

    # ipython will dump out here. with non-zero exitcode. blah...
    print "Running Argparse --help"
    p.parsers[0].parser.parse_args(["--help"])

    return p
