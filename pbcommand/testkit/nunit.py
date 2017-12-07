#!/usr/bin/env python

"""
Generate an NUnit XML test report annotated with test issue keys, suitable
for importing into JIRA/X-ray
"""

# NOTE: deliberately avoiding any dependencies outside the standard library!
from xml.dom import minidom
import logging
import argparse
import sys

log = logging.getLogger(__name__)


class TestCase(object):
    """
    Container for the results of an executed test.
    """
    def __init__ (self, name, success, tests=(), requirements=(),
                  asserts=1):
        self.name = name
        self.success = success
        self.tests = list(tests)
        self.requirements = list(requirements)
        self.asserts = asserts

    def to_xml(self, doc):
        """Create xml.dom.minidom node in document"""
        test = doc.createElement("test-case")
        test.setAttribute("name", self.name)
        test.setAttribute("executed", "True")
        test.setAttribute("time", "0.001")
        test.setAttribute("asserts", str(self.asserts))
        test.setAttribute("success", str(self.success))
        if self.success:
            test.setAttribute("result", "Success")
        else:
            test.setAttribute("result", "Error")
        if len(self.tests) > 0 or len(self.requirements) > 0:
            properties = doc.createElement("properties")
            for test_key in self.tests:
                prop = doc.createElement("property")
                prop.setAttribute("name", "Test")
                prop.setAttribute("value", test_key)
                properties.appendChild(prop)
            for req in self.requirements:
                prop = doc.createElement("property")
                prop.setAttribute("name", "Requirement")
                prop.setAttribute("value", req)
                properties.appendChild(prop)
            test.appendChild(properties)
        return test

    @staticmethod
    def from_xml(node):
        tests, requirements = [], []
        for prop in node.getElementsByTagName("property"):
            property_type = prop.getAttribute("name")
            if property_type == "Requirement":
                requirements.append(prop.getAttribute("value"))
            elif property_type == "Test":
                tests.append(prop.getAttribute("value"))
        return TestCase(
            name=node.getAttribute("name"),
            success=node.getAttribute("success") == "True",
            tests=tests,
            requirements=requirements,
            asserts=int(node.getAttribute("asserts")))


def create_nunit_xml(test_cases):
    """
    Create overall NUnit XML output for a list of test cases
    """
    passed = [t.success for t in test_cases].count(True)
    failed = [t.success for t in test_cases].count(False)
    doc = minidom.Document()
    root = doc.createElement("test-results")
    root.setAttribute("total", str(passed+failed))
    root.setAttribute("failed", str(failed))
    root.setAttribute("passed", str(passed))
    suite = doc.createElement("test-suite")
    attr_result = "Success"
    if failed != 0:
        attr_result = "Error"
    root.setAttribute("result", attr_result)
    suite.setAttribute("result", attr_result)
    doc.appendChild(root)
    root.appendChild(suite)
    results = doc.createElement("results")
    suite.appendChild(results)
    for test_case in test_cases:
        results.appendChild(test_case.to_xml(doc))
    return doc


def combine_results(xml_files):
    test_cases = []
    for xml_file in xml_files:
        log.info("Reading NUnit report %s", xml_file)
        dom = minidom.parse(xml_file)
        for node in dom.getElementsByTagName("test-case"):
            test_cases.append(TestCase.from_xml(node))
    return create_nunit_xml(test_cases)


def main(argv):
    """Standalone program runner"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("test_name")
    parser.add_argument("test_key",
                        help="JIRA issue associated with this test")
    parser.add_argument("--failed", action="store_false", dest="success",
                        default=True, help="Indicate test failure")
    parser.add_argument("--output-xml", action="store",
                        default="nunit_out.xml",
                        help="NUnit XML file to write")
    args = parser.parse_args(argv)
    test_case = TestCase(args.test_name, args.success, [args.test_key])
    doc = create_nunit_xml([test_case])
    with open(args.output_xml, "w") as xml_out:
        xml_out.write(doc.toprettyxml(indent="  "))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
