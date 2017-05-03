import sys
import pprint
import logging

from pbcommand.models import FileTypes, OutputFileType
from pbcommand.cli import registry_builder, registry_runner, QuickOpt

log = logging.getLogger(__name__)

# Version of the Commandline Tool
__version__ = "0.1.2"

registry = registry_builder("pbcommand", "python -m pbcommand.cli.examples.dev_quick_hello_world ")


def _example_main(input_files, output_files, **kwargs):
    log.info("Running example main with {i} {o} kw:{k}".format(i=input_files,
                                                               o=output_files, k=kwargs))
    # write mock output files, otherwise the End-to-End test will fail
    xs = output_files if isinstance(output_files, (list, tuple)) else [output_files]
    for x in xs:
        with open(x, 'w') as writer:
            if len(kwargs) > 0:
                pprint.pprint(dict(kwargs), writer)
            else:
                writer.write("Mock data\n")
    return 0


@registry("dev_qhello_world", "0.2.1", FileTypes.FASTA, FileTypes.FASTA, nproc=1, options=dict(alpha=1234))
def run_quick_hello_world_rtc(rtc):
    log.debug("Dev Quick Hello World Example. Fasta -> Fasta with option alpha=1234")
    return _example_main(rtc.task.input_files[0], rtc.task.output_files[0], nproc=rtc.task.nproc)


@registry("dev_fastq2fasta", "0.1.0", FileTypes.FASTQ, FileTypes.FASTA,
          options=dict(beta=QuickOpt(1234.0, "Beta Name", "Beta Description"), gamma=True),
          name="Fastq to Fasta", description="Dev Task Fastq to Fasta Example")
def run_dev_fastq_to_fasta_rtc(rtc):
    return _example_main(rtc.task.input_files[0], rtc.task.output_files[0])


@registry("dev_txt_hello", "0.1.0", FileTypes.TXT, (FileTypes.TXT, FileTypes.TXT), nproc=3, is_distributed=False)
def run_dev_txt_hello_rtc(rtc):
    return _example_main(rtc.task.input_files, rtc.task.output_files)


@registry("dev_test_options", "0.1.0", FileTypes.TXT, FileTypes.TXT,
          nproc=1,
          options=dict(alpha=1234, beta=5.4321, gamma=True, ploidy=("haploid", "diploid"), delta=(1,2,3), epsilon=(0.01,0.1,1.0)))
def run_dev_test_options_rtc(rtc):
    log.debug("Dev Quick Hello World Example with various option types")
    return _example_main(rtc.task.input_files[0], rtc.task.output_files[0], options=rtc.task.options)


def _to_output(i, file_type):
    default_name = "_".join([file_type.file_type_id, file_type.base_name + "_" + str(i)])
    label = "label_" + file_type.file_type_id
    desc = "File {f}".format(f=file_type)
    return OutputFileType(file_type.file_type_id, label, repr(file_type), desc, default_name)


def _to_outputs(file_types):
    return [_to_output(i, ft) for i, ft in enumerate(file_types)]


@registry("dev_txt_custom_outs", "0.1.0", FileTypes.TXT, _to_outputs((FileTypes.TXT, FileTypes.TXT)), name="Custom Txt Task")
def run_dev_txt_custom_outs_rtc(rtc):
    """Test for using OutputFileTypes as outputs

    Output types can be specified as FileType, or OutputFileType instances
    """
    return _example_main(rtc.task.input_files, rtc.task.output_files)


if __name__ == '__main__':
    default_log_level = logging.DEBUG
    sys.exit(registry_runner(registry,
                             sys.argv[1:],
                             default_log_level=default_log_level,
                             version=__version__))
