import logging
import unittest

import pbcommand
from pbcommand.models.common import (PacBioFloatChoiceOption, PacBioIntOption,
                                     PacBioStringOption,
                                     PacBioStringChoiceOption,
                                     PacBioIntChoiceOption,
                                     PacBioBooleanOption, PacBioFloatOption)

log = logging.getLogger(__name__)


def _to_i(s):
    return "test.task_options.{}".format(s)


def get_or(i, value):
    return value if i is None else i


class TestPacBioBasicOptionTest(unittest.TestCase):
    OPT_KLASS = PacBioIntOption
    OPT_ID = "alpha"
    OPT_NAME = "Alpha"
    OPT_DESC = "Alpha description"
    OPT_DEFAULT = 2

    def _to_opt(self, i=None, n=None, v=None, d=None):
        ix = get_or(i, _to_i(self.OPT_ID))
        name = get_or(n, self.OPT_NAME)
        value = get_or(v, self.OPT_DEFAULT)
        description = get_or(d, self.OPT_DESC)
        return self.OPT_KLASS(ix, name, value, description)

    def test_sanity_option(self):
        o = self._to_opt()
        log.debug("Created option {o}".format(o=o))

        self.assertEqual(
            o.option_id,
            "test.task_options.{}".format(
                self.OPT_ID))
        self.assertEqual(o.name, self.OPT_NAME)
        self.assertEqual(o.default, self.OPT_DEFAULT)
        self.assertEqual(o.description, self.OPT_DESC)


class TestPacBioIntOptionTest(TestPacBioBasicOptionTest):

    def test_bad_value_string(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v="bad-string")

    def test_bad_value_float(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v=3.124)

    def test_bad_value_boolean(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v=True)


class TestPacBioBooleanOptionTest(TestPacBioBasicOptionTest):
    OPT_KLASS = PacBioBooleanOption
    OPT_DEFAULT = True

    def test_bad_value_int(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v=1)

    def test_bad_value_float(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v=1.10)

    def test_bad_value_string(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v="bad-string")


class TestPacBioFloatOptionTest(TestPacBioBasicOptionTest):
    OPT_KLASS = PacBioFloatOption
    OPT_DEFAULT = 3.1415

    def test_coerced_value_int(self):
        o = self._to_opt(v=1)
        self.assertEqual(o.default, 1.0)

    def test_bad_value_boolean(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v=True)

    def test_bad_value_string(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v="bad-string")

    def test_bad_value_float_tuple(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v=(1.0, 2.0))


class TestPacBioStringOptionTest(TestPacBioBasicOptionTest):
    OPT_KLASS = PacBioStringOption
    OPT_DEFAULT = "gamma"

    def test_bad_value_int(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v=1)

    def test_bad_value_float(self):
        with self.assertRaises(TypeError):
            _ = self._to_opt(v=1.10)


class TestPacBioBasicChoiceTest(TestPacBioBasicOptionTest):
    OPT_KLASS = PacBioStringChoiceOption
    OPT_CHOICES = ("alpha", "beta", "gamma")
    OPT_DEFAULT = "beta"
    OPT_BAD_OPTION = "delta"

    def _to_opt(self, i=None, n=None, v=None, d=None, c=None):
        ix = get_or(i, _to_i(self.OPT_ID))
        name = get_or(n, self.OPT_NAME)
        value = get_or(v, self.OPT_DEFAULT)
        description = get_or(d, self.OPT_DESC)
        choices = get_or(c, self.OPT_CHOICES)
        return self.OPT_KLASS(ix, name, value, description, choices)

    def test_sanity_choice_option(self):
        o = self._to_opt()
        self.assertEqual(o.choices, self.OPT_CHOICES)

    def test_bad_invalid_choice(self):
        with self.assertRaises(ValueError):
            _ = self._to_opt(v=self.OPT_BAD_OPTION)


class TestPacBioChoiceStringOptionTest(TestPacBioBasicChoiceTest):
    OPT_KLASS = PacBioStringChoiceOption
    OPT_DEFAULT = "gamma"
    OPT_BAD_OPTION = "Bad-value"


class TestPacBioIntChoiceOptionTest(TestPacBioBasicChoiceTest):
    OPT_KLASS = PacBioIntChoiceOption
    OPT_CHOICES = (1, 2, 7)
    OPT_DEFAULT = 2
    OPT_BAD_OPTION = 3


class TestPacBioFloatChoiceOptionTest(TestPacBioBasicChoiceTest):
    OPT_KLASS = PacBioFloatChoiceOption
    OPT_CHOICES = (1.0, 2.0, 7.0)
    OPT_DEFAULT = 2.0
    OPT_BAD_OPTION = -1.0

    def test_coerce_float_choices(self):
        choices = (10, 12123, 12)
        o = self._to_opt(c=choices, v=12)

    def test_bad_choices(self):
        choices = (1, 2.0, "bad-value")
        with self.assertRaises(TypeError):
            _ = self._to_opt(c=choices)
