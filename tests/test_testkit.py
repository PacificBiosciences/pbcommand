from pbcommand.testkit.base_utils import pb_requirements


class TestBaseUtils:

    def test_pb_requirements_decorator(self):
        """
        Test that the pb_requirements decorator monkey-patches test methods
        correctly.
        """
        class MyTestClass:
            @pb_requirements("SL-1")
            def test_1(self):
                assert True

            @pb_requirements("SL-2")
            def test_2(self):
                self.fail("Fail!")

        methods = [
            MyTestClass.test_1,
            MyTestClass.test_2,
        ]
        requirements = [m.__pb_requirements__ for m in methods]
        assert requirements == [['SL-1'], ['SL-2']]
