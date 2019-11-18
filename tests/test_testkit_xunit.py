from xml.etree import ElementTree
import subprocess
import tempfile
import unittest
import shutil

from pbcommand.testkit import pb_requirements
from pbcommand.testkit import xunit as X

try:
    import avro
except ImportError:
    avro = None

skip_unless_avro_installed = unittest.skipUnless(avro is not None,
                                                 "avro not installed")

def _get_and_run_test_suite():
    class MyTestClass(unittest.TestCase):
        @pb_requirements("SL-1")
        def test_1(self):
            self.assertTrue(True)
        @pb_requirements("SL-2")
        def test_2(self):
            self.assertTrue(False)
        @pb_requirements("SL-3", "SL-4")
        @unittest.skip("Skipped")
        def test_3(self):
            self.assertTrue(True)
    suite = unittest.TestLoader().loadTestsFromTestCase(MyTestClass)
    suite._cleanup = False
    result = unittest.TestResult()
    suite.run(result)
    return suite, result


def _generate_jenkins_xml(test_name):
    suite, result = _get_and_run_test_suite()
    x = X.convert_suite_and_result_to_xunit([suite], result,
        name="pbcommand.tests.test_xunit_output")
    xunit_file = tempfile.NamedTemporaryFile(suffix=".xml").name
    with open(xunit_file, "w") as xml_out:
        xml_out.write(x.toxml())
    return X.xunit_file_to_jenkins(xunit_file, test_name)


class TestXunitOutput(unittest.TestCase):

    @skip_unless_avro_installed
    def test_convert_to_xunit(self):
        suite, result = _get_and_run_test_suite()
        x = X.convert_suite_and_result_to_xunit([suite], result,
            name="pbcommand.tests.test_xunit_output")
        root = ElementTree.fromstring(x.toxml())
        self.assertEqual(root.tag, "testsuite")
        self.assertEqual(root.attrib["failures"], "1")
        self.assertEqual(root.attrib["skip"], "1")
        tests = list(root.findall("testcase"))
        self.assertEqual(len(tests), 3)
        requirements = []
        for el in root.findall("properties"):
            for p in el.findall("property"):
                requirements.append(p.attrib["value"])
        self.assertEqual(sorted(requirements), ["SL-1","SL-2","SL-3","SL-4"])

    @skip_unless_avro_installed
    def test_xunit_file_to_jenkins(self):
        j = _generate_jenkins_xml("my_testkit_job")
        root = ElementTree.fromstring(j.toxml())
        requirements = []
        for el in root.findall("properties"):
            for p in el.findall("property"):
                requirements.append(p.attrib["value"])
        self.assertEqual(sorted(requirements), ["SL-1","SL-2","SL-3","SL-4"])

    def _get_junit_files(self):
        x1 = tempfile.NamedTemporaryFile(suffix=".xml").name
        x2 = tempfile.NamedTemporaryFile(suffix=".xml").name
        for file_name, job_name in zip([x1, x2], ["job_1", "job_2"]):
            with open(file_name, "w") as x:
                x.write(_generate_jenkins_xml(job_name).toxml())
        return (x1, x2)

    @skip_unless_avro_installed
    def test_merge_junit_files(self):
        x1, x2 = self._get_junit_files()
        x_merged = tempfile.NamedTemporaryFile(suffix=".xml").name
        X.merge_junit_files(x_merged, [x1, x2])
        x = ElementTree.ElementTree(file=x_merged)
        suites = x.getroot()
        assert suites.tag == "testsuites", suites.tag
        job_names = [el.attrib['name'] for el in suites.findall("testsuite")]
        self.assertEqual(job_names, ["job_1", "job_2"])
        # now try combining testsuites (just re-using the already merged file)
        x_merged2 = tempfile.NamedTemporaryFile(suffix=".xml").name
        shutil.copyfile(x_merged, x_merged2)
        x_merged3 = tempfile.NamedTemporaryFile(suffix=".xml").name
        X.merge_junit_files(x_merged3, [x_merged, x_merged2])
        x = ElementTree.ElementTree(file=x_merged3)
        suites = x.getroot()
        assert suites.tag == "testsuites", suites.tag
        job_names = [el.attrib['name'] for el in suites.findall("testsuite")]
        self.assertEqual(job_names, ["job_1", "job_2", "job_1", "job_2"])

    @skip_unless_avro_installed
    def test_merge_junit_files_cmdline(self):
        x1, x2 = self._get_junit_files()
        x_merged = tempfile.NamedTemporaryFile(suffix=".xml").name
        args = ["python", "-m" "pbcommand.testkit.merge_junit_files",
                "-o", x_merged, x1, x2, "--quiet"]
        self.assertEqual(subprocess.call(args), 0)
        x = ElementTree.ElementTree(file=x_merged)
        suites = x.getroot()
        assert suites.tag == "testsuites", suites.tag
        job_names = [el.attrib['name'] for el in suites.findall("testsuite")]
        self.assertEqual(job_names, ["job_1", "job_2"])
