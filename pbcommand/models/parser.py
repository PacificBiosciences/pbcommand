""" Commandline Parser for Tools. Supports Tool Contracts

# Author: Michael Kocher
"""
import abc
import os
import logging
import argparse
import functools
import re

# there's a problem with functools32 and jsonschema. This import raise an
# import error.
#import jsonschema
import datetime

from pbcommand.common_options import (add_base_options_with_emit_tool_contract,
                                      add_subcomponent_versions_option)
from .tool_contract import ToolDriver

log = logging.getLogger(__name__)

__version__ = "0.1.1"

__all__ = ["PbParser",
           "PyParser",
           "ToolContractParser",
           "get_default_contract_parser"]

RX_TASK_ID = re.compile(r'^([A-z0-9_]*)\.tasks\.([A-z0-9_]*)$')
RX_TASK_OPTION_ID = re.compile(r'^([A-z0-9_]*)\.task_options\.([A-z0-9_\.]*)')


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


def _validate_option_or_cast(dtype, dvalue):
    if isinstance(dvalue, dtype):
        return dvalue
    else:
        # XXX this is almost always going to be the case...
        if isinstance(dvalue, basestring):
            try:
                return dtype(dvalue)
            except ValueError as e:
                pass
        raise TypeError("Invalid option type: '{a}' provided, '{e}' "
                        "expected".format(a=dvalue, e=dtype))


def _validate_option(dtype, dvalue):
    if isinstance(dvalue, dtype):
        return dvalue
    else:
        raise TypeError("Invalid option type: '{a}' provided, '{e}' "
                        "expected".format(a=dvalue, e=dtype))


def _validate_id(prog, idtype, tid):
    if prog.match(tid):
        return tid
    else:
        raise ValueError("Invalid format {t}: '{i}'".format(t=idtype, i=tid))

_validate_task_id = functools.partial(_validate_id, RX_TASK_ID, 'task id')
_validate_task_option_id = functools.partial(_validate_id, RX_TASK_OPTION_ID,
                                             'task option id')


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

    _validate_task_option_id(option_id)

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
        self.tool_id = _validate_task_id(tool_id)
        self.version = version
        self.description = description

    def __repr__(self):
        _d = dict(k=self.__class__.__name__, i=self.tool_id, v=self.version)
        return "<{k} id:{i} {v} >".format(**_d)

    @abc.abstractmethod
    def add_input_file_type(self, file_type, file_id, name, description):
        """
        Add a mandatory input file parameter.  On the Python argparse side,
        this will be a positional argument.

        :param file_type: file type ID from pbcommand.models.common, e.g.
                          FileTypes.DS_REF
        :param file_id: parameter name, mainly used on argparse side
        :param name: plain-English name
        :param description: help string
        """
        raise NotImplementedError

    @abc.abstractmethod
    def add_output_file_type(self, file_type, file_id, name, description, default_name):
        """
        Add a mandatory output file parameter.  On the Python argparse side,
        this will be a positional argument.

        :param file_type: file type ID from pbcommand.models.common, e.g.
                          FileTypes.DS_REF
        :param file_id: parameter name, mainly used on argparse side
        :param name: plain-English name
        :param description: help string
        :param default_name: tuple of form (base_name, extension) specifying
                             the default output file name
        """
        raise NotImplementedError

    @abc.abstractmethod
    def add_int(self, option_id, option_str, default, name, description):
        """
        Add an optional integer keyword argument (e.g. "--n=10" or "--n 10" on
        the command line).

        :param option_id: fully-qualified option name used in tool contract
                          layer, of form "pbcommand.task_options.my_option"
        :param option_str: shorter parameter name, mainly used in Python
                           argparse layer, but *without* leading dashes
        :param default: default value (must be an actual integer, not None)
        :param name: plain-English name
        :param description: help string
        """
        raise NotImplementedError

    @abc.abstractmethod
    def add_float(self, option_id, option_str, default, name, description):
        """
        Add an optional float keyword argument (e.g. "--n=10" or "--n 10" on
        the command line).

        :param option_id: fully-qualified option name used in tool contract
                          layer, of form "pbcommand.task_options.my_option"
        :param option_str: shorter parameter name, mainly used in Python
                           argparse layer, but *without* leading dashes
        :param default: default value (must be an actual number, not None)
        :param name: plain-English name
        :param description: help string
        """
        raise NotImplementedError

    @abc.abstractmethod
    def add_str(self, option_id, option_str, default, name, description):
        """
        Add a generic keyword argument whose type is a string.

        :param option_id: fully-qualified option name used in tool contract
                          layer, of form "pbcommand.task_options.my_option"
        :param option_str: shorter parameter name, mainly used in Python
                           argparse layer, but *without* leading dashes
        :param default: default value (can be blank, but not None)
        :param name: plain-English name
        :param description: help string
        """
        raise NotImplementedError

    @abc.abstractmethod
    def add_boolean(self, option_id, option_str, default, name, description):
        """
        Add a boolean option.

        :param option_id: fully-qualified option name used in tool contract
                          layer, of form "pbcommand.task_options.my_option"
        :param option_str: shorter parameter name, mainly used in Python
                           argparse layer, but *without* leading dashes
        :param default: specifies the boolean value of this option **if the
                        argument was supplied**, i.e. on the argparse layer,
                        default=True is equivalent to action="store_true"
        :param name: plain-English name
        :param description: help string
        """
        raise NotImplementedError

_validate_argparse_int = functools.partial(_validate_option_or_cast, int)
_validate_argparse_float = functools.partial(_validate_option_or_cast, float)
_validate_argparse_bool = functools.partial(_validate_option_or_cast, bool)
_validate_argparse_str = functools.partial(_validate_option_or_cast, str)


class PyParser(PbParserBase):
    """PbParser backed that supports argparse"""

    def __init__(self, tool_id, version, description, subcomponents=()):
        super(PyParser, self).__init__(tool_id, version, description)
        self.parser = argparse.ArgumentParser(version=version,
                                              description=description,
                                              formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                              add_help=True)
        if subcomponents:
            add_subcomponent_versions_option(self.parser, subcomponents)

    def add_input_file_type(self, file_type, file_id, name, description):
        # this will propagate up the label to the exception
        vfunc = functools.partial(_validate_file, file_id)
        self.parser.add_argument(file_id, type=vfunc, help=description)

    def add_output_file_type(self, file_type, file_id, name, description, default_name):
        self.parser.add_argument(file_id, type=str, help=description)

    def add_int(self, option_id, option_str, default, name, description):
        # FIXME Need to better define and validate option_str
        opt = "--" + option_str
        self.parser.add_argument(opt, type=_validate_argparse_int,
                                 help=description,
                                 default=_validate_argparse_int(default))

    def add_float(self, option_id, option_str, default, name, description):
        opt = "--" + option_str
        self.parser.add_argument(opt, type=_validate_argparse_float,
                                 help=description,
                                 default=_validate_argparse_float(default))

    def add_str(self, option_id, option_str, default, name, description):
        # Fixme
        opt = "--" + option_str
        self.parser.add_argument(opt, type=_validate_argparse_str,
                                 help=description,
                                 default=_validate_argparse_str(default))

    def add_boolean(self, option_id, option_str, default, name, description):
        d = {True: "store_true", False: "store_false"}
        opt = '--' + option_str
        self.parser.add_argument(opt, action=d[_validate_argparse_bool(default)],
                                 help=description)


class ToolContractParser(PbParserBase):
    """Parser to support Emitting and running ToolContracts"""

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
        self.options.append(to_option_schema(option_id,
                                             JsonSchemaTypes.INT, name, description,
                                             _validate_option(int, default)))

    def add_float(self, option_id, option_str, default, name, description):
        self.options.append(to_option_schema(option_id,
                                             JsonSchemaTypes.NUM, name, description,
                                             _validate_option(float, default)))

    def add_str(self, option_id, option_str, default, name, description):
        self.options.append(to_option_schema(option_id,
                                             JsonSchemaTypes.STR, name, description,
                                             _validate_option(str, default)))

    def add_boolean(self, option_id, option_str, default, name, description):
        self.options.append(to_option_schema(option_id,
                                             JsonSchemaTypes.BOOL, name, description,
                                             _validate_option(bool, default)))

    def to_tool_contract(self):

        created_at = datetime.datetime.now()
        _t = dict(input_types=self.input_types,
                  output_types=self.output_types,
                  task_type=self.task_type,
                  schema_options=self.options,
                  nproc=self.nproc_symbol,
                  resource_types=self.resource_types,
                  _comment="Created by v{v} at {d}".format(v=__version__, d=created_at.isoformat()))

        _d = dict(version=self.version,
                  tool_contract_id=self.tool_id,
                  driver=self.driver.to_dict(),
                  tool_contract=_t)
        return _d


class PbParser(PbParserBase):
    """
    Wrapper class for managing separate tool contract and argument parsers
    (stored as tool_contract_parser and arg_parser attributes respectively).
    """

    def __init__(self, tool_contract_parser, arg_parser, *parsers):
        """

        :param tool_contract_parser:
        :type tool_contract_parser: ToolContractParser
        :param arg_parser:
        :type arg_parser: PyParser
        :param parsers:
        :return:
        """

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

    def _dispatch(self, f_name, args, kwds):
        for parser in self.parsers:
            f = getattr(parser, f_name)
            f(*args, **kwds)

    def add_input_file_type(self, file_type, file_id, name, description):
        args = file_type, file_id, name, description
        self._dispatch("add_input_file_type", args, {})

    def add_output_file_type(self, file_type, file_id, name, description, default_name):
        args = file_type, file_id, name, description, default_name
        self._dispatch("add_output_file_type", args, {})

    def add_int(self, option_id, option_str, default, name, description):
        args = option_id, option_str, default, name, description
        self._dispatch("add_int", args, {})

    def add_float(self, option_id, option_str, default, name, description):
        args = option_id, option_str, default, name, description
        self._dispatch("add_float", args, {})

    def add_str(self, option_id, option_str, default, name, description):
        args = option_id, option_str, default, name, description
        self._dispatch("add_str", args, {})

    def add_boolean(self, option_id, option_str, default, name, description):
        args = option_id, option_str, default, name, description
        self._dispatch("add_boolean", args, {})

    def to_contract(self):
        return self.tool_contract_parser.to_tool_contract()


def get_default_contract_parser(tool_id, version, description, driver_exe,
                                task_type, nproc_symbol, resource_types,
                                subcomponents=()):
    """
    Central point of creating a Tool contract that can emit and run tool
    contracts.

    :returns: PbParser object
    """
    driver = ToolDriver(driver_exe)
    arg_parser = PyParser(tool_id, version, description,
                          subcomponents=subcomponents)
    tc_parser = ToolContractParser(tool_id, version, description, task_type, driver, nproc_symbol, resource_types)
    return PbParser(tc_parser, arg_parser)
