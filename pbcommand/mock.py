import json
import logging
import os
import random
import string
import tempfile

from pbcore.io import FastaRecord, FastqRecord, FastaWriter, FastqWriter
from pbcommand.models.report import Report, Attribute

log = logging.getLogger(__name__)


class Constants(object):
    SEQ = ('A', 'C', 'G', 'T')


def _random_dna_sequence(min_length=100, max_length=1000):
    n = random.choice(list(xrange(min_length, max_length)))
    return "".join([random.choice(Constants.SEQ) for _ in xrange(n)])


def _to_fasta_record(header, seq):
    return FastaRecord(header, seq)


def _to_fastq_record(header, seq):
    quality = [ord(random.choice(string.ascii_letters)) for _ in seq]
    return FastqRecord(header, seq, quality=quality)


def __to_fastx_records(n, _to_seq_func, _to_record_func, prefix="record"):
    """

    :param n:
    :param _to_seq_func: () => DNA seq
    :param _to_record_func: (header, dna_seq) => Record
    :return: Fastq/Fasta Record
    """
    for i in xrange(n):
        header = "{p}_{i}".format(p=prefix, i=i)
        seq = _to_seq_func()
        r = _to_record_func(header, seq)
        yield r


def to_fasta_records(n, prefix="record"):
    return __to_fastx_records(n, _random_dna_sequence, _to_fasta_record,
        prefix=prefix)


def to_fastq_records(n, prefix="record"):
    return __to_fastx_records(n, _random_dna_sequence, _to_fastq_record,
        prefix=prefix)


def write_fastx_records(fastx_writer_klass, records, path):
    n = 0
    with fastx_writer_klass(path) as w:
        for record in records:
            n += 1
            w.writeRecord(record)

    log.debug("Completed writing {n} records to {p}".format(n=n, p=path))
    return 0


def write_random_fasta_records(path, nrecords=100, prefix="record"):
    return write_fastx_records(FastaWriter, to_fasta_records(nrecords,
        prefix=prefix), path)


def write_random_fastq_records(path, nrecords=100, prefix="record"):
    return write_fastx_records(FastqWriter, to_fastq_records(nrecords,
        prefix=prefix), path)


def _to_random_tmp_fofn(nrecords):
    def _to_f(name):
        suffix = "".join([name, '.fofn'])
        t = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
        t.close()
        return t.name

    paths = []
    for x in xrange(nrecords):
        path = _to_f("random_{i}".format(i=x))
        paths.append(path)

    return paths


def write_fofn(path, file_paths):
    with open(path, 'w') as w:
        w.write("\n".join([str(f) for f in file_paths]))
    return 0


def write_random_fofn(path, nrecords):
    """Write a generic fofn"""

    fofns = _to_random_tmp_fofn(nrecords)
    write_fofn(path, fofns)
    return fofns


def write_random_report(path, nrecords):

    attributes = [Attribute("mock_attr_{i}".format(i=i), i, name="Attr {i}".format(i=i)) for i in xrange(nrecords)]
    r = Report("mock_report", attributes=attributes)
    r.write_json(path)
    return r


def write_generic_txt_file(path, nrecords):
    with open(path, 'w') as w:
        for i in xrange(nrecords):
            w.write("Record-{i}".format(i=i))

    return 0


def write_mock_file_by_type(path, nrecords):
    _, ext = os.path.splitext(path)
    _d = {".fastq": write_random_fastq_records,
          ".fasta": write_random_fasta_records,
          ".json": write_random_report,
          ".fofn": write_random_fofn}
    func = _d.get(ext, write_generic_txt_file)
    func(path, nrecords)
    return 0
