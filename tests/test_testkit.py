import unittest

from pbcommand.testkit.base_utils import pb_requirements


class TestBaseUtils(unittest.TestCase):

    def test_pb_requirements_decorator(self):
        """
        Test that the pb_requirements decorator monkey-patches test methods
        correctly.
        """
        class MyTestClass(unittest.TestCase):
            @pb_requirements("SL-1")
            def test_1(self):
                self.assertTrue(True)

            @pb_requirements("SL-2")
            def test_2(self):
                self.fail("Fail!")
        tests = unittest.TestLoader().loadTestsFromTestCase(MyTestClass)
        methods = [getattr(t, t._testMethodName) for t in tests]
        requirements = [m.__pb_requirements__ for m in methods]
        self.assertEqual(requirements, [['SL-1'], ['SL-2']])
