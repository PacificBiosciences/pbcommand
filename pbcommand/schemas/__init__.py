import functools
import os
import warnings

SCHEMA_REGISTRY = {}

__all__ = [
    'validate_pbreport',
    'validate_datastore_view_rules',
    'SCHEMA_REGISTRY',
]


def _load_schema(idx, name):
    try:
        try:
            from avro.schema import Parse as parse
        except ImportError:
            from avro.schema import parse
        # warnings.warn("Avro support is deprecated and will be removed",
        #              DeprecationWarning)
        d = os.path.dirname(__file__)
        schema_path = os.path.join(d, name)
        with open(schema_path, 'r') as f:
            schema = parse(f.read())
        SCHEMA_REGISTRY[idx] = schema
        return schema
    except ImportError:
        return None


PBREPORT_SCHEMA = _load_schema("pbreport", "pbreport.avsc")
PRESET_SCHEMA = _load_schema("pipeline_presets", "pipeline_presets.avsc")
PRESET_SCHEMA2 = _load_schema(
    "pipeline_presets_simple",
    "pipeline_presets_simple.avsc")
DS_VIEW_SCHEMA = _load_schema(
    "datastore_view_rules",
    "datastore_view_rules.avsc")
REPORT_SPEC_SCHEMA = _load_schema("report_spec", "report_spec.avsc")


def _validate(schema, msg, d):
    try:
        """Validate a python dict against a avro schema"""
        try:
            from avro.io import Validate as validate
        except ImportError:
            from avro.io import validate
        # warnings.warn("Avro support is deprecated and will be removed",
        #              DeprecationWarning)
        # FIXME(mkocher)(2016-7-16) Add a better error message than "Invalid"
        if not validate(schema, d):
            raise IOError("Invalid {m} ".format(m=msg))
        return True
    except ImportError:
        raise IOError("Invalid {m} ".format(m=msg))


def _is_valid(schema, d):
    try:
        from avro.io import Validate as validate
    except ImportError:
        from avro.io import validate
    # warnings.warn("Avro support is deprecated and will be removed",
    #              DeprecationWarning)
    return validate(schema, d)


validate_pbreport = functools.partial(
    _validate, PBREPORT_SCHEMA, "Report Model")
validate_report = validate_pbreport
validate_datastore_view_rules = functools.partial(
    _validate, DS_VIEW_SCHEMA, "Pipeline DataStore View Rules")
validate_report_spec = functools.partial(
    _validate,
    REPORT_SPEC_SCHEMA,
    "Report Specification Model")


def validate_presets(d):
    if not isinstance(d.get("options"), dict):
        return _validate(PRESET_SCHEMA, "Pipeline Presets Model", d)
    else:
        return _validate(
            PRESET_SCHEMA2, "Pipeline Presets Model (Simplified)", d)


is_valid_report = functools.partial(_is_valid, PBREPORT_SCHEMA)
is_valid_presets = functools.partial(_is_valid, PRESET_SCHEMA)
is_valid_datastore_view_rules = functools.partial(_is_valid, DS_VIEW_SCHEMA)
is_valid_report_spec = functools.partial(_is_valid, REPORT_SPEC_SCHEMA)
