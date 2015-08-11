import shlex
import subprocess
import sys
import logging

from pbcommand.models import FileTypes
from pbcommand.cli.quick import registry_builder, registry_runner

log = logging.getLogger(__name__)

registry = registry_builder("tool_ns", "python -m pbcommand.cli.examples.dev_quick_hello_world ")


def _example_main(*args, **kwargs):
    log.info("Running example main with {a} kw:{k}".format(a=args, k=kwargs))
    return 0


@registry("hello_world", "0.2.1", FileTypes.FASTA, FileTypes.FASTA, nproc=1)
def run_rtc(rtc):
    return _example_main(rtc.task.input_files[0], rtc.task.output_files[0], rtc.task.nproc)


@registry("fastq2fasta", "0.1.0", FileTypes.FASTQ, FileTypes.FASTA)
def run_rtc(rtc):
    return _example_main(rtc.task.input_files[0], rtc.task.output_files[0])


if __name__ == '__main__':
    sys.exit(registry_runner(registry, sys.argv[1:]))
