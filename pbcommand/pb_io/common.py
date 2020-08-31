import json
import logging
import sys
import warnings

from pbcommand.models import (PipelineChunk, PipelineDataStoreViewRules,
                              TaskOptionTypes, PacBioFloatChoiceOption,
                              PacBioStringChoiceOption,
                              PacBioIntChoiceOption, PacBioStringOption,
                              PacBioFloatOption, PacBioBooleanOption,
                              PacBioIntOption, PipelinePreset, EntryPoint)
from pbcommand.models.legacy import Pipeline
from pbcommand.schemas import validate_datastore_view_rules, validate_presets
from pbcommand import to_ascii, to_utf8

log = logging.getLogger(__name__)


def json_path_or_d(value):
    if isinstance(value, dict):
        return value
    elif isinstance(value, ("""s""".__class__, u"""s""".__class__)):
        with open(value, 'r') as f:
            d = json.loads(f.read())
        return d
    else:
        raise ValueError("Unsupported value. Expected dict, or string")


def _json_path_or_d(func):
    def _wrapper(value):
        return func(json_path_or_d(value))
    return _wrapper


def write_pipeline_chunks(chunks, output_json_file, comment):

    _d = dict(nchunks=len(chunks), _version="0.1.0",
              chunks=[c.to_dict() for c in chunks])

    if comment is not None:
        _d['_comment'] = comment

    with open(output_json_file, 'w') as f:
        f.write(json.dumps(_d, indent=4, separators=(',', ': ')))

    log.debug(
        "Write {n} chunks to {o}".format(
            n=len(chunks),
            o=output_json_file))


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
        chunks.sort(key=lambda a: a.chunk_id)
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
    desc = to_utf8(d['description'])

    klass_map = {TaskOptionTypes.CHOICE_STR: PacBioStringChoiceOption,
                 TaskOptionTypes.CHOICE_FLOAT: PacBioFloatChoiceOption,
                 TaskOptionTypes.CHOICE_INT: PacBioIntChoiceOption}

    k = klass_map[option_type_id]

    # Sanitize Unicode hack
    if k is PacBioStringChoiceOption:
        default_value = to_ascii(default_value)
        choices = [to_ascii(i) for i in choices]

    opt = k(opt_id, name, default_value, desc, choices)

    return opt


def __simple_option_by_type(
        option_id, name, default, description, option_type_id):

    option_type = TaskOptionTypes.from_simple_str(option_type_id)

    klass_map = {TaskOptionTypes.INT: PacBioIntOption,
                 TaskOptionTypes.FLOAT: PacBioFloatOption,
                 TaskOptionTypes.STR: PacBioStringOption,
                 TaskOptionTypes.BOOL: PacBioBooleanOption,
                 TaskOptionTypes.FILE: PacBioStringOption}

    k = klass_map[option_type]

    # This requires a hack for the unicode to ascii for string option type.
    if k is PacBioStringOption:
        # sanitize unicode
        default = to_ascii(default)

    opt = k(option_id, name, default, description)
    return opt


def _pacbio_legacy_option_from_dict(d):
    """
    Load the legacy (jsonschema-ish format)

    Note, choice types are not supported here.

    :rtype: PacBioOption
    """
    warnings.warn(
        "This is obsolete and will disappear soon",
        DeprecationWarning)

    opt_id = d['pb_option']['option_id']
    name = d['pb_option']['name']
    default = d['pb_option']['default']
    desc = d['pb_option']['description']
    option_type_id = to_ascii(d['pb_option']['type'])

    # Hack to support "number"
    if option_type_id == "number":
        option_type_id = "float"

    return __simple_option_by_type(opt_id, name, default, desc, option_type_id)


def _pacbio_option_from_dict(d):
    if "pb_option" in d:
        return _pacbio_legacy_option_from_dict(d)
    else:
        return __simple_option_by_type(
            d['id'],
            d['name'],
            d['default'],
            to_utf8(d['description']),
            d['optionTypeId'])


def pacbio_option_from_dict(d):
    """Fundamental API for loading any PacBioOption type from a dict """
    # This should probably be pushed into pbcommand/pb_io/* for consistency
    # Extensions are supported by adding a dispatch method by looking for
    # required key(s) in the dict.
    if "choices" in d and d.get('choices') is not None:
        # the None check is for the TCs that are non-choice based models, but
        # were written with "choices" key
        return _pacbio_choice_option_from_dict(d)
    else:
        return _pacbio_option_from_dict(d)


# XXX this could probably be more robust
@_json_path_or_d
def load_pipeline_presets_from(d):
    """
    Load pipeline presets from dictionary.  This expects a schema where the
    options are arrays of type (id,value,optionTypeId), but it will also accept
    a shorthand where the options are dictionaries.
    """
    validate_presets(d)
    options = d['options']
    if isinstance(options, list):
        options = {o['id']: o['value'] for o in options}
    taskOptions = d['taskOptions']
    if isinstance(taskOptions, list):
        taskOptions = {o['id']: o['value'] for o in taskOptions}
    presets = PipelinePreset(
        options=options,
        task_options=taskOptions,
        pipeline_id=d['pipelineId'],
        preset_id=d['presetId'],
        name=d.get('name', None),
        description=d.get('description', None))
    return presets


@_json_path_or_d
def load_pipeline_interface_from(d):
    bindings = {}  # obsolete
    epts = [EntryPoint.from_dict(d) for d in d["entryPoints"]]
    opts = [pacbio_option_from_dict(o) for o in d["taskOptions"]]
    return Pipeline(d['id'],
                    d['name'],
                    d['version'],
                    d['description'],
                    bindings,
                    epts,
                    tags=d['tags'],
                    task_options=opts)
