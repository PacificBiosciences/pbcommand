import os
import avro.schema

SCHEMA_REGISTRY = {}


def _load_schema(name):

    d = os.path.dirname(__file__)
    schema_path = os.path.join(d, name)
    with open(schema_path, 'r') as f:
        schema = avro.schema.parse(f.read())
    SCHEMA_REGISTRY[name] = schema
    return schema

RTC_SCHEMA = _load_schema("resolved_tool_contract.avsc")
PBREPORT_SCHEMA = _load_schema("pbreport.avsc")