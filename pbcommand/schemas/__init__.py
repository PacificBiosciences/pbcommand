import os

import functools

import avro.schema
from avro.io import validate

SCHEMA_REGISTRY = {}

__all__ = ['validate_pbreport',
           'validate_tc',
           'validate_rtc',
           'validate_datastore_view_rules',
           'SCHEMA_REGISTRY']


def _load_schema(idx, name):

    d = os.path.dirname(__file__)
    schema_path = os.path.join(d, name)
    with open(schema_path, 'r') as f:
        schema = avro.schema.parse(f.read())
    SCHEMA_REGISTRY[idx] = schema
    return schema

RTC_SCHEMA = _load_schema("resolved_tool_contract", "resolved_tool_contract.avsc")
PBREPORT_SCHEMA = _load_schema("pbreport", "pbreport.avsc")
TC_SCHEMA = _load_schema("tool_contract", "tool_contract.avsc")
PRESET_SCHEMA = _load_schema("pipeline_presets", "pipeline_presets.avsc")
PRESET_SCHEMA2 = _load_schema("pipeline_presets_simple", "pipeline_presets_simple.avsc")
DS_VIEW_SCHEMA = _load_schema("datastore_view_rules", "datastore_view_rules.avsc")
REPORT_SPEC_SCHEMA = _load_schema("report_spec", "report_spec.avsc")


def _validate(schema, msg, d):
    """Validate a python dict against a avro schema"""
    # FIXME(mkocher)(2016-7-16) Add a better error message than "Invalid"
    if not validate(schema, d):
        raise IOError("Invalid {m} ".format(m=msg))
    return True


def _is_valid(schema, d):
    return validate(schema, d)


validate_rtc = functools.partial(_validate, RTC_SCHEMA, "Resolved Tool Contract Model")
validate_pbreport = functools.partial(_validate, PBREPORT_SCHEMA, "Report Model")
validate_report = validate_pbreport
validate_tc = functools.partial(_validate, TC_SCHEMA, "Tool Contract Model")
validate_datastore_view_rules = functools.partial(_validate, DS_VIEW_SCHEMA, "Pipeline DataStore View Rules")
validate_report_spec = functools.partial(_validate, REPORT_SPEC_SCHEMA, "Report Specification Model")


def validate_presets(d):
    if not isinstance(d.get("options"), dict):
        return _validate(PRESET_SCHEMA, "Pipeline Presets Model", d)
    else:
        return _validate(PRESET_SCHEMA2, "Pipeline Presets Model (Simplified)", d)

is_valid_rtc = functools.partial(_is_valid, RTC_SCHEMA)
is_valid_report = functools.partial(_is_valid, PBREPORT_SCHEMA)
is_valid_tc = functools.partial(_is_valid, TC_SCHEMA)
is_valid_presets = functools.partial(_is_valid, PRESET_SCHEMA)
is_valid_datastore_view_rules = functools.partial(_is_valid, DS_VIEW_SCHEMA)
is_valid_report_spec = functools.partial(_is_valid, REPORT_SPEC_SCHEMA)
