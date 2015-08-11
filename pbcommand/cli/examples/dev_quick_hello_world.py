import sys
import logging

from pbcommand.models import FileTypes
from pbcommand.cli import registry_builder, registry_runner

log = logging.getLogger(__name__)

registry = registry_builder("pbcommand", "python -m pbcommand.cli.examples.dev_quick_hello_world ")


def _example_main(input_files, output_files, **kwargs):
    log.info("Running example main with {i} {o} kw:{k}".format(i=input_files,
                                                               o=output_files, k=kwargs))
    # write mock output files, otherwise the End-to-End test will fail
    xs = output_files if isinstance(output_files, (list, tuple)) else [output_files]
    for x in xs:
        with open(x, 'w') as writer:
            writer.write("Mock data\n")
    return 0


@registry("dev_qhello_world", "0.2.1", FileTypes.FASTA, FileTypes.FASTA, nproc=1)
def run_rtc(rtc):
    return _example_main(rtc.task.input_files[0], rtc.task.output_files[0], nproc=rtc.task.nproc)


@registry("dev_fastq2fasta", "0.1.0", FileTypes.FASTQ, FileTypes.FASTA)
def run_rtc(rtc):
    return _example_main(rtc.task.input_files[0], rtc.task.output_files[0])


if __name__ == '__main__':
    sys.exit(registry_runner(registry, sys.argv[1:]))
