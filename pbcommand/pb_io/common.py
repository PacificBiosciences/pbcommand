import logging
import json
import sys
import warnings

from pbcommand.models import (PipelineChunk, PipelineDataStoreViewRules,
                              TaskOptionTypes, PacBioFloatChoiceOption,
                              PacBioStringChoiceOption,
                              PacBioIntChoiceOption, PacBioStringOption,
                              PacBioFloatOption, PacBioBooleanOption,
                              PacBioIntOption)
from pbcommand.schemas import validate_datastore_view_rules

log = logging.getLogger(__name__)


def write_pipeline_chunks(chunks, output_json_file, comment):

    _d = dict(nchunks=len(chunks), _version="0.1.0",
              chunks=[c.to_dict() for c in chunks])

    if comment is not None:
        _d['_comment'] = comment

    with open(output_json_file, 'w') as f:
        f.write(json.dumps(_d, indent=4, separators=(',', ': ')))

    log.debug("Write {n} chunks to {o}".format(n=len(chunks), o=output_json_file))


def load_pipeline_chunks_from_json(path):
    """Returns a list of Pipeline Chunks


    :rtype: list[PipelineChunk]
    """

    try:
        with open(path, 'r') as f:
            d = json.loads(f.read())

        chunks = []
        for cs in d['chunks']:
            chunk_id = cs['chunk_id']
            chunk_datum = cs['chunk']
            c = PipelineChunk(chunk_id, **chunk_datum)
            chunks.append(c)
        return chunks
    except Exception:
        msg = "Unable to load pipeline chunks from {f}".format(f=path)
        sys.stderr.write(msg + "\n")
        raise


def load_pipeline_datastore_view_rules_from_json(path):
    """Load pipeline presets from dict"""
    with open(path, 'r') as f:
        d = json.loads(f.read())
        validate_datastore_view_rules(d)
        return PipelineDataStoreViewRules.from_dict(d)


def _pacbio_choice_option_from_dict(d):
    """
    Factory/dispatch method for returning a PacBio Choice Option Type

    :rtype: PacBioOption
    """
    choices = d['choices']
    default_value = d['default']
    # this will immediately raise
    option_type_id = TaskOptionTypes.from_choice_str(d['optionTypeId'])

    opt_id = d['id']
    name = d['name']
    desc = d['description'].encode("UTF-8")

    klass_map = {TaskOptionTypes.CHOICE_STR: PacBioStringChoiceOption,
                 TaskOptionTypes.CHOICE_FLOAT: PacBioFloatChoiceOption,
                 TaskOptionTypes.CHOICE_INT: PacBioIntChoiceOption}

    k = klass_map[option_type_id]

    # Sanitize Unicode hack
    if k is PacBioStringChoiceOption:
        default_value = default_value.encode('ascii', 'ignore')
        choices = [i.encode('ascii', 'ignore') for i in choices]

    opt = k(opt_id, name, default_value, desc, choices)

    return opt


def __simple_option_by_type(option_id, name, default, description, option_type_id):

    option_type = TaskOptionTypes.from_simple_str(option_type_id)

    klass_map = {TaskOptionTypes.INT: PacBioIntOption,
                 TaskOptionTypes.FLOAT: PacBioFloatOption,
                 TaskOptionTypes.STR: PacBioStringOption,
                 TaskOptionTypes.BOOL: PacBioBooleanOption}

    k = klass_map[option_type]

    # This requires a hack for the unicode to ascii for string option type.
    if k is PacBioStringOption:
        # sanitize unicode
        default = default.encode('ascii', 'ignore')

    opt = k(option_id, name, default, description)
    return opt


def _pacbio_legacy_option_from_dict(d):
    """
    Load the legacy (jsonschema-ish format)

    Note, choice types are not supported here.

    :rtype: PacBioOption
    """
    warnings.warn("This is obsolete and will disappear soon", DeprecationWarning)

    opt_id = d['pb_option']['option_id']
    name = d['pb_option']['name']
    default = d['pb_option']['default']
    desc = d['pb_option']['description']
    option_type_id = d['pb_option']['type'].encode('ascii')

    # Hack to support "number"
    if option_type_id == "number":
        option_type_id = "float"

    return __simple_option_by_type(opt_id, name, default, desc, option_type_id)


def _pacbio_option_from_dict(d):
    if "pb_option" in d:
        return _pacbio_legacy_option_from_dict(d)
    else:
        return __simple_option_by_type(d['id'], d['name'], d['default'], d['description'].encode("UTF-8"), d['optionTypeId'])


def pacbio_option_from_dict(d):
    """Fundamental API for loading any PacBioOption type from a dict """
    # This should probably be pushed into pbcommand/pb_io/* for consistency
    # Extensions are supported by adding a dispatch method by looking for required
    # key(s) in the dict.
    if "choices" in d and d.get('choices') is not None:
        # the None check is for the TCs that are non-choice based models, but
        # were written with "choices" key
        return _pacbio_choice_option_from_dict(d)
    else:
        return _pacbio_option_from_dict(d)
