import os
import sys

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

version = __import__('pbcommand').get_version()

_REQUIREMENTS_TEST_FILE = "REQUIREMENTS_TEST.txt"
_README = 'README.md'


def _get_description():
    with open(_get_local_file(_README)) as f:
        _long_description = f.read()
    return _long_description


def _get_local_file(file_name):
    return os.path.join(os.path.dirname(__file__), file_name)


install_deps = [
    "avro-python3",
    "requests",
    "iso8601",
    "pytz",
    "future"
]

test_deps = [
    "pytest",
    "pytest-xdist",
    "pytest-cov",
    "nose",
    "coverage",
    "tox",
    "pep8",
    "autopep8",
    "pylint"
]


setup(
    name='pbcommand',
    version=version,
    license='BSD',
    author='mpkocher natechols',
    author_email='nechols@pacificbiosciences.com',
    url="https://github.com/PacificBiosciences/pbcommand",
    download_url='https://github.com/PacificBiosciences/pbcommand/tarball/{v}'.format(v=version),
    description='Library and Tools for interfacing to PacBio pbsmrtpipe workflow engine.',
    install_requires=install_deps,
    tests_require=test_deps,
    long_description=_get_description(),
    keywords='workflow pacbio'.split(),
    packages=find_packages(),
    package_data={"pbcommand": ["schemas/*.avsc"]},
    zip_safe=False,
    extras_require={"pbcore": ["pbcore", "ipython", "autopep8"],
                    "interactive": ['prompt_toolkit']},
    classifiers=['Development Status :: 4 - Beta',
                 'Environment :: Console',
                 'Topic :: Software Development :: Bug Tracking']
)
