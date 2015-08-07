import os

import functools

import avro.schema
from avro.io import validate

SCHEMA_REGISTRY = {}

__all__ = ['validate_pbreport',
           'validate_tc',
           'validate_rtc',
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


def _validate(schema, d):
    """Validate a python dict against a avro schema"""
    return validate(schema, d)

validate_rtc = functools.partial(_validate, RTC_SCHEMA)
validate_pbreport = functools.partial(_validate, PBREPORT_SCHEMA)
validate_tc = functools.partial(_validate, TC_SCHEMA)