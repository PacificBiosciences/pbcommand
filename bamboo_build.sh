#!/bin/bash -ex

mkdir -p tmp
/opt/python-2.7.9/bin/python /mnt/software/v/virtualenv/13.0.1/virtualenv.py tmp/venv
source tmp/venv/bin/activate
pip install -r REQUIREMENTS.txt
pip install -r REQUIREMENTS_TEST.txt
pip install nose
python setup.py install
nosetests -s --verbose --with-xunit --xunit-file=nosetests.xml --logging-config log_nose.cfg tests/test_*.py
