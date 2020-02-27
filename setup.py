#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

test_deps = [
    'autopep8',
    'coverage',
    'pep8',
    'pylint',
    'pytest',
    'pytest-cov',
    'pytest-xdist',
]

setup(
    name='pbcommand',
    version='2.0.1',
    author='Pacific Biosciences',
    author_email='devnet@pacificbiosciences.com',
    description='Library and Tools for interfacing with PacBio® data CLI tools',
    license='BSD-3-Clause-Clear',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Topic :: Software Development :: Bug Tracking',
    ],
    install_requires=[
        'avro-python3',
        'iso8601',
        'pytz',
        'requests',
    ],
    test_requires=test_deps,
    extras_require={
        'test': test_deps,
        'pbcore': [
            'pbcore',
            'ipython',
            'autopep8',
        ],
        'interactive': [
            'prompt_toolkit',
        ]},
    python_requires='>=3.7',
)
