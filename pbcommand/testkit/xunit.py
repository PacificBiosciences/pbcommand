"""IO layer for converting a TestResult instance to XUnit and an XUnit.xml
parser"""

from __future__ import absolute_import, division, print_function

import os
import re
import copy
import logging
import datetime
import unittest

from xml.etree.ElementTree import Element, ElementTree, ParseError

from xmlbuilder import XMLBuilder

log = logging.getLogger(__name__)

_RESULT_STATES = 'success skipped error failure'.split()


class XunitTestSuite(object):

    def __init__(self, name, tests, run_time=None, pb_requirements=()):
        """

        :param name: (str) Name of test suite
        :param tests: (list) of XunitTestCase instances
        """
        self.name = name
        for t in tests:
            if not isinstance(t, XunitTestCase):
                raise TypeError("Expected {k} got {t}.".format(t=type(t), k=XunitTestCase.__class__.__name__))
        self._tests = tests
        self._run_time = run_time
        self._pb_requirements = pb_requirements

    @staticmethod
    def from_xml(file_name):
        if os.path.exists(file_name):
            file_name = os.path.abspath(file_name)
            suite = _parser(file_name)
            return suite
        else:
            raise IOError("Unable to find {x}".format(x=file_name))

    @property
    def tests(self):
        return self._tests

    def _get_tests_by_result(self, result):
        if result in _RESULT_STATES:
            return [test for test in self.tests if test.result == result]
        else:
            raise ValueError("{r} is Invalid state. Supported states {s}.".format(r=result, s=_RESULT_STATES))

    def __len__(self):
        return len(self.tests)

    def __repr__(self):
        return "<{c} tests:{n} successful:{s} failed:{f} errors:{e} skipped:{sk} >".format(c=self.__class__.__name__, n=self.ntests, s=self.nsuccess, f=self.nfailure, e=self.nerrors, sk=self.nskipped)

    def __str__(self):
        outs = list()
        outs.append("TestName : {n}".format(n=self.name))
        outs.append("{c} tests {n} successful {s} failed {f} errors {e} skipped {sk}".format(c=self.__class__.__name__, n=self.ntests, s=self.nsuccess, f=self.nfailure, e=self.nerrors, sk=self.nskipped))
        for i, t in enumerate(self.tests):
            outs.append(" : ".join([str(i + 1).rjust(5), str(t)]))
        outs.append("")
        outs.append("End of Summary.")
        return "\n".join(outs)

    @property
    def ntests(self):
        return len(self.tests)

    @property
    def errors(self):
        return self._get_tests_by_result('error')

    @property
    def nerrors(self):
        return len(self.errors)

    @property
    def skipped(self):
        return self._get_tests_by_result('skipped')

    @property
    def nskipped(self):
        return len(self.skipped)

    @property
    def success(self):
        return self._get_tests_by_result('success')

    @property
    def nsuccess(self):
        return len(self.success)

    @property
    def failure(self):
        return self._get_tests_by_result('failure')

    @property
    def nfailure(self):
        return len(self.failure)

    @property
    def requirements(self):
        return self._pb_requirements

    def to_xml(self):
        """Return an XML instance of the suite"""

        x = XMLBuilder('testsuite', name=self.name, tests=str(self.ntests), errors=str(self.nerrors), failures=str(self.nfailure), skip=str(self.nskipped))

        for test_case in self.tests:
            classname = test_case.classname
            # sanitize for XML
            text = "" if test_case.text is None else test_case.text
            etype = "" if test_case.etype is None else test_case.etype

            with x.testcase(classname=classname, name=test_case.name,
                            result=test_case.result, etype=etype,
                            text=text):

                if test_case.result in 'failures':
                    x.error(type=test_case.etype, message=test_case.message)
                elif test_case.result == 'failure':
                    x.failure(type=test_case.etype, message=test_case.message)
                elif test_case.result == 'skipped':
                    x.skipped(type=test_case.etype, message=test_case.message)
                elif test_case.result == 'error':
                    x.error(type=test_case.etype, message=test_case.message)
                else:
                    # Successful testcase
                    pass
        if len(self._pb_requirements):
            with x.properties():
                for req in self._pb_requirements:
                    x.property(name="Requirement", value=req)
        return x

    def to_dict(self):
        was_successful = self.ntests == self.nsuccess
        d = dict(nfailures=self.nfailure, nsuccess=self.nsuccess,
                 nskipped=self.nskipped, ntests=self.ntests,
                 nerrors=self.nerrors, was_successful=was_successful,
                 tests=[t.to_dict() for t in self.tests])
        return d


class XunitTestCase(object):
    RESULTS = 'success skipped error failure'.split()

    def __init__(self, classname, name, result, etype=None, text=None,
                 message=None, run_time=None):
        self.classname = classname
        self.name = name
        if result in XunitTestCase.RESULTS:
            self.result = result
        else:
            raise ValueError("{r} is Invalid state. Supported states {s}.".format(r=result, s=_RESULT_STATES))
        self.text = text
        self.etype = etype
        self.message = message
        self.run_time = run_time

    def __str__(self):
        outs = list()
        x = [self.result.rjust(10), self.name]
        outs.append(" : ".join(x))
        if self.result != 'success':
            outs.append("\n\n {x}\n".format(x=self.message))
        return " ".join(outs)

    def __repr__(self):
        return "<{k} {n} {r}>".format(k=self.classname, n=self.name, r=self.result)

    def to_dict(self):
        was_successful = self.result == 'success'
        d = dict(classname=self.classname, name=self.name,
                 text=self.text, etype=self.etype,
                 result=self.result,
                 message=self.message, run_time=self.run_time,
                 was_successful=was_successful)
        return d


def to_timedelta(val):
    if val is None:
        return None
    return datetime.timedelta(seconds=float(val))


def _parser(file_name):
    """Parse the nose xml file and return a XunitTestSuite"""
    xml = ElementTree(file=file_name)
    root = xml.getroot()

    if root.tag == 'testsuite':
        suite_name = root.attrib['name']
        # suite_run_time = root.attrib['time']
        suite_run_time = None
        tests = []

        # iterator over every test case
        for el in root.findall('testcase'):
            classname = el.attrib['classname']
            name = el.attrib['name']
            result = 'success'
            text = None
            message = None
            etype = None
            rtime = None

            for e in el:
                if e.tag in ('failure', 'skipped', 'error'):
                    result = e.tag
                    text = e.text
                    message = e.attrib['message']
                    etype = e.attrib['type']

            # t = (classname, name, result, text, message, etype)
            t = XunitTestCase(classname, name, result, text=text,
                              message=message, etype=etype)
            tests.append(t)

        # Properties linking to JIRA issues
        pb_requirements = []
        for el in root.findall("properties"):
            for p in el.findall("property"):
                pb_requirements.append(p.attrib['value'])

        xunit_test_suite = XunitTestSuite(suite_name, tests,
                                          run_time=suite_run_time,
                                          pb_requirements=pb_requirements)

    else:
        msg = "Unable to find tag 'testsuite' in {f}".format(f=file_name)
        log.error(msg)
        raise ValueError(msg)

    # log.debug(xunit_test_suite)
    return xunit_test_suite


def convert_suite_and_result_to_xunit(suite,
                                      result,
                                      name="PysivXunitTestSuite",
                                      requirements=()):
    """Custom a test suite and result to XML.

    The name is used to set the xml suitename for jenkins.

    <testsuite errors="1" failures="1" name="1234" skip="1" tests="5">

    :param suite: unittest.TestSuite
    :param result: unittest.TestResult
    :param name:

    :return: XML instance
    """

    # When a test fails in setUpClass, the result is a
    # unittest.suite._ErrorHolder rather than a TestCase. We need to handle
    # those differently.
    def parse_setupclass_error(klass_id):
        """Return what's inside the parentheses."""
        return re.search("(?<=\().*(?=\))", klass_id).group(0)

    # Test cls names/id
    names = 'errors skipped failures'.split()
    klass_results = {}
    for n in names:
        klass_results[n] = []
        for klass, out in getattr(result, n):
            if isinstance(klass, unittest.suite._ErrorHolder):
                klass_results[n].append(parse_setupclass_error(klass.id()))
            else:
                klass_results[n].append(klass.id())

    nskipped = len(klass_results['skipped'])
    nerrors = len(klass_results['errors'])
    nfailures = len(klass_results['failures'])

    def _to_key(test_case):
        # If the test_case is an _ErrorHolder, the key should be parsed from
        # the description. It won't have a _testMethodName
        if isinstance(test_case, unittest.suite._ErrorHolder):
            return parse_setupclass_error(test_case.description)
        m = test_case.__module__
        n = test_case.__class__.__name__
        mn = test_case._testMethodName
        # d = test_case._testMethodDoc
        # return m, n, mn, d
        return ".".join([m, n, mn])

    requirements = set(requirements)
    all_test_cases = {}
    for s in suite:
        if isinstance(s, unittest.suite.TestSuite):
            for tc in s:
                key = _to_key(tc)
                all_test_cases[key] = None
                m = getattr(tc, tc._testMethodName)
                requirements.update(getattr(m, "__pb_requirements__", []))
        else:
            raise TypeError("Unsupported test suite case ({x})".format(x=type(s)))

    ntests = len(all_test_cases)

    # loop over failures, errors, skipped, assign message
    for n in names:
        # get all testcases with state
        ts = getattr(result, n)
        for t, msg in ts:
            k = _to_key(t)
            all_test_cases[k] = msg

    # import ipdb; ipdb.set_trace()

    # Create XML
    x = XMLBuilder('testsuite', name=name, tests=str(ntests),
                   errors=str(nerrors), failures=str(nfailures),
                   skip=str(nskipped))

    for idx, message in all_test_cases.iteritems():
        test_method = idx.split('.')[-1]
        with x.testcase(classname=idx, name=test_method, time="1.000"):
            if idx in klass_results['errors']:
                x.error(type="exceptions.Exception", message=message)
            elif idx in klass_results['failures']:
                x.failure(type="exceptions.Exception", message=message)
            elif idx in klass_results['skipped']:
                # print "skipped", idx
                x.skipped(type="unittest.case.SkipTest", message=message)
            else:
                # print "Success", idx
                pass
    with x.properties() as p:
        for req in sorted(list(requirements)):
            p.property(name="Requirement", value=req)
    return x


def xunit_file_to_jenkins(xunit_file, job_name):
    """
    To better support jenkins, the standard Xunit file needs to be modified in
    two fundamental ways.

    1. The testsuite.name is change to the job name
    2. The job name/id is appended to every testcase.

    :param xunit_file:
    :param job_name:
    :return: xml instance
    """
    xsuite = XunitTestSuite.from_xml(xunit_file)

    tests = []
    for test_case in xsuite.tests:
        t = copy.copy(test_case)
        t.name = "_".join([test_case.name, job_name])
        t.classname = "_".join([test_case.classname, job_name])
        tests.append(t)

    jenkins_suite = XunitTestSuite(job_name, tests,
                                   pb_requirements=xsuite.requirements)
    return jenkins_suite.to_xml()


def merge_junit_files(output_file, input_files):
    root_out = Element("testsuites")
    xml_out = ElementTree(root_out)
    for input_file in input_files:
        xml_in = ElementTree(file=input_file)
        root = xml_in.getroot()
        if root.tag == 'testsuite':
            root_out.append(root)
        else:
            assert root.tag == "testsuites"
            for suite in root.findall("testsuite"):
                root_out.append(suite)
    with open(output_file, "w") as x:
        xml_out.write(x)
