#!/usr/bin/env python
from __future__ import unicode_literals, print_function
import os
import sys
import warnings

from pbcommand.cli import get_default_argparser
from pbcommand.models import SymbolTypes
from pbcommand.pb_io import (load_tool_contract_from,
                             write_resolved_tool_contract,
                             write_resolved_tool_contract_avro)

from pbcommand.resolver import resolve_tool_contract

try:
    from prompt_toolkit.filters import Always
    from prompt_toolkit.shortcuts import get_input
except ImportError:
    sys.stderr.write("interactive resolver requires 'prompt_toolkit' (pip install prompt_toolkit)\n")
    raise


def run_main(tc):
    """:type tc: ToolContract"""
    print("Loaded tc {c}".format(c=tc))

    if tc.task.nproc == SymbolTypes.MAX_NPROC:
        nproc = get_input('Enter max nproc: ')
    else:
        # not quite right
        nproc = 1

    output_dir = get_input('Output Directory: ', enable_system_bindings=Always())
    output_dir = os.path.abspath(output_dir)

    input_files = []
    for i, input_type in enumerate(tc.task.input_file_types):
        in_path = get_input(" {i} file {p} path :".format(i=i, p=input_type))
        if not os.path.exists(in_path):
            warnings.warn("Unable to find {p}".format(p=in_path))

        # Make sure all inputs are abspaths
        p = in_path if os.path.isabs(in_path) else os.path.abspath(in_path)
        input_files.append(p)

    tool_options = {}
    rtc = resolve_tool_contract(tc, input_files, output_dir, '/tmp', int(nproc), tool_options, is_distributable=False)
    print(rtc)

    def to_n(ext):
        return "resolved_tool_contract." + ext

    def to_f(ext):
        return "_".join([tc.task.task_id, to_n(ext)])

    def to_p(ext):
        return os.path.join(output_dir, to_f(ext))

    rtc_path = to_p("json")
    print("writing RTC to {f}".format(f=rtc_path))

    # Always write the JSON RTC file
    write_resolved_tool_contract(rtc, rtc_path)

    if rtc.driver.serialization.lower() == "avro":
        avro_rtc_path = to_p("avro")
        print("writing AVRO RTC to {f}".format(f=avro_rtc_path))
        write_resolved_tool_contract_avro(rtc, avro_rtc_path)

    return rtc


def _run_main(args):
    return run_main(load_tool_contract_from(args.tc_path))


def get_parser():
    p = get_default_argparser("0.1.0", "Interactive tool for resolving Tool Contracts")
    p.add_argument("tc_path", type=str, help='Path to Tool Contract')
    p.set_defaults(func=_run_main)
    return p


def main(argv=sys.argv):
    p = get_parser()
    args = p.parse_args(argv[1:])
    args.func(args)
    return 0


if __name__ == '__main__':
    sys.exit(main())
