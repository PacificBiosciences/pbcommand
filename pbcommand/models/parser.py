""" Commandline Parser for Tools. Supports Tool Contracts

# Author: Michael Kocher
"""
import abc
import os
import logging
import argparse
import functools

# there's a problem with functools32 and jsonschema. This import raise an
# import error.
#import jsonschema

from pbcommand.common_options import add_base_options_with_emit_tool_contract
from .tool_contract import ToolDriver

log = logging.getLogger(__name__)

__version__ = "0.1.0"

__all__ = ["PbParser",
           "PyParser",
           "ToolContractParser",
           "get_default_contract_parser"]


def _to_file_type(format_):
    return "pacbio.file_types.{x}".format(x=format_)


class JsonSchemaTypes(object):
    # array is a native type, but not supported
    BOOL = "boolean"
    INT = "integer"
    NUM = "number"
    STR = "string"
    NULL = "null"
    OBJ = "object"

    # Optional values e.g., Option[String]
    OPT_BOOL = [BOOL, NULL]
    OPT_INT = [INT, NULL]
    OPT_STR = [STR, NULL]
    OPT_NUM = [NUM, NULL]


def _validate_file(label, path):
    if os.path.exists(path):
        return os.path.abspath(path)
    else:
        raise IOError("Unable to find '{x}' file '{p}'".format(x=label, p=path))


def to_opt_id(namespace, s):
    return ".".join([namespace, "options", s])


def validate_value(schema, v):
    import jsonschema
    return jsonschema.validate(v, schema)


def is_valid(schema, v):
    """Returns a bool if the schema is valid"""
    import jsonschema
    try:
        validate_value(schema, v)
        return True
    except jsonschema.ValidationError:
        pass
    return False


def validate_schema(f):
    """Deco for validate the returned jsonschema against Draft 4 of the spec"""
    def w(*args, **kwargs):
        schema = f(*args, **kwargs)
        import jsonschema
        _ = jsonschema.Draft4Validator(schema)
        return schema
    return w


@validate_schema
def to_option_schema(option_id, dtype_or_dtypes, display_name, description, default_value):
    """
    Simple util factory method
    :param dtype_or_dtypes: single data type or list of data types
    :param option_id: globally unique task option id. Must begin with
    'pbsmrtpipe.task_options.'
    :param display_name: display name of task options
    :param description: Short description of the task options
    :param required: Is the option required.
    """
    # annoying that you can't specify a tuple
    if isinstance(dtype_or_dtypes, tuple):
        dtype_or_dtypes = list(dtype_or_dtypes)

    d = {'$schema': "http://json-schema.org/draft-04/schema#",
         'type': 'object',
         'title': "JSON Schema for {o}".format(o=option_id),
         'properties': {option_id: {'description': description,
                                    'title': display_name,
                                    'type': dtype_or_dtypes}}
         }

    d['required'] = [option_id]
    d['properties'][option_id]['default'] = default_value
    return d


class PbParserBase(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, tool_id, version, description):
        self.tool_id = tool_id
        self.version = version
        self.description = description

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.tool_id, v=self.version)
        return "<{k} id:{i} {v} >".format(**_d)

    @abc.abstractmethod
    def add_input_file_type(self, input_file_type_id, file_id, name, description):
        raise NotImplementedError

    @abc.abstractmethod
    def add_output_file_type(self, input_file_type_id, file_id, name, description, default_name):
        raise NotImplementedError

    @abc.abstractmethod
    def add_int(self, option_id, option_str, default, name, description):
        raise NotImplementedError

    @abc.abstractmethod
    def add_str(self, option_id, option_str, default, name, description):
        raise NotImplementedError

    @abc.abstractmethod
    def add_boolean(self, option_id, option_str, default, name, description):
        raise NotImplementedError


class PyParser(PbParserBase):

    def __init__(self, tool_id, version, description):
        super(PyParser, self).__init__(tool_id, version, description)
        self.parser = argparse.ArgumentParser(version=version,
                                              description=description,
                                              formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                              add_help=True)

    def add_input_file_type(self, file_type_id, file_id, name, description):
        # this will propagate up the label to the exception
        vfunc = functools.partial(_validate_file, file_id)
        self.parser.add_argument(file_id, type=vfunc, help=description)

    def add_output_file_type(self, file_type_id, file_id, name, description, default_name):
        self.parser.add_argument(file_id, type=str, help=description)

    def add_int(self, option_id, option_str, default, name, description):
        # Fixme
        opt = "--" + option_str
        self.parser.add_argument(opt, type=int, help=description, default=default)

    def add_str(self, option_id, option_str, default, name, description):
        # Fixme
        opt = "--" + option_str
        self.parser.add_argument(opt, type=str, help=description)

    def add_boolean(self, option_id, option_str, default, name, description):
        d = {True: "store_true", False: "store_false"}
        opt = '--' + option_str
        self.parser.add_argument(opt, action=d[default], help=description)


class ToolContractParser(PbParserBase):

    def __init__(self, tool_id, version, description, task_type, driver, nproc_symbol, resource_types):
        super(ToolContractParser, self).__init__(tool_id, version, description)
        self.input_types = []
        self.output_types = []
        self.options = []
        self.driver = driver
        self.nproc_symbol = nproc_symbol
        self.resource_types = resource_types
        self.task_type = task_type

    def add_input_file_type(self, file_type, file_id, name, description):
        _d = dict(file_type_id=file_type.file_type_id, id=file_id, title=name, description=description)
        self.input_types.append(_d)

    def add_output_file_type(self, file_type, file_id, name, description, default_name):
        _d = dict(file_type_id=file_type.file_type_id, id=file_id, title=name, description=description, default_name=default_name)
        self.output_types.append(_d)

    def add_int(self, option_id, option_str, default, name, description):
        self.options.append(to_option_schema(option_id, JsonSchemaTypes.INT, name, description, default))

    def add_str(self, option_id, option_str, default, name, description):
        self.options.append(to_option_schema(option_id, JsonSchemaTypes.STR, name, description, default))

    def add_boolean(self, option_id, option_str, default, name, description):
        self.options.append(to_option_schema(option_id, JsonSchemaTypes.BOOL, name, description, default))

    def to_tool_contract(self):
        _t = dict(input_types=self.input_types,
                  output_types=self.output_types,
                  task_type=self.task_type,
                  schema_options=self.options,
                  nproc=self.nproc_symbol,
                  resource_types=self.resource_types,
                  _comment="Created by v{v}".format(v=__version__))

        _d = dict(version=self.version,
                  tool_contract_id=self.tool_id,
                  driver=self.driver.to_dict(),
                  tool_contract=_t)
        return _d


class PbParser(PbParserBase):

    def __init__(self, tool_contract_parser, arg_parser, *parsers):

        # Tool Contract Parser
        self.tool_contract_parser = tool_contract_parser

        # python wrapper parser.
        self.arg_parser = arg_parser
        # add options, so it will show up via --help
        add_base_options_with_emit_tool_contract(self.arg_parser.parser)

        # a list of other parsers that adhere to the PbParserBase interface
        # can be used.
        self.other_parsers = parsers

        # for now assume parsers have the same version, id, ...
        tool_id = tool_contract_parser.tool_id
        version = tool_contract_parser.version
        description = tool_contract_parser.description
        super(PbParser, self).__init__(tool_id, version, description)


    @property
    def parsers(self):
        return [self.tool_contract_parser, self.arg_parser]

    def _dispatch(self, f_name, args):
        for parser in self.parsers:
            f = getattr(parser, f_name)
            f(*args)

    def add_input_file_type(self, file_type_id, file_id, name, description):
        args = file_type_id, file_id, name, description
        self._dispatch("add_input_file_type", args)

    def add_output_file_type(self, input_file_type_id, file_id, name, description, default_name):
        args = input_file_type_id, file_id, name, description, default_name
        self._dispatch("add_output_file_type", args)

    def add_int(self, option_id, option_str, default, name, description):
        args = option_id, option_str, default, name, description
        self._dispatch("add_int", args)

    def add_str(self, *args):
        self._dispatch("add_str", args)

    def add_boolean(self, *args):
        self._dispatch("add_boolean", args)

    def to_contract(self):
        return self.tool_contract_parser.to_tool_contract()


def get_default_contract_parser(tool_id, version, description, driver_exe, task_type, nproc_symbol, resource_types):
    """Central point of creating a Tool contract that can emit and run tool contracts"""
    driver = ToolDriver(driver_exe)
    arg_parser = PyParser(tool_id, version, description)
    tc_parser = ToolContractParser(tool_id, version, description, task_type, driver, nproc_symbol, resource_types)
    return PbParser(tc_parser, arg_parser)
