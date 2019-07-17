#!/bin/bash

type module >& /dev/null || . /mnt/software/Modules/current/init/bash
module purge
module load gcc
module load ccache
module load python/2
#module load make

set -vex
which gcc
which g++
gcc --version
which python
python --version

export PYTHONUSERBASE=$(pwd)/LOCAL
export PATH=${PYTHONUSERBASE}/bin:${PATH}

PIP="pip --cache-dir=$bamboo_build_working_directory/.pip"
if [[ -z ${bamboo_repository_branch_name+x} ]]; then
  WHEELHOUSE=/mnt/software/p/python/wheelhouse/develop
elif [[ ${bamboo_repository_branch_name} == develop ]]; then
  WHEELHOUSE=/mnt/software/p/python/wheelhouse/develop
elif [[ ${bamboo_repository_branch_name} == master ]]; then
  WHEELHOUSE=/mnt/software/p/python/wheelhouse/master
else
  WHEELHOUSE=/mnt/software/p/python/wheelhouse/develop
fi
export WHEELHOUSE

rm -rf   build
mkdir -p build/{bin,lib,include,share}
PIP_INSTALL="${PIP} install --no-index --find-links=${WHEELHOUSE}"
$PIP_INSTALL --user -r  REQUIREMENTS.txt
$PIP_INSTALL --user -e ./
$PIP_INSTALL --user -r  REQUIREMENTS_TEST.txt
$PIP_INSTALL --user pbtestdata pbcore
#nosetests -s --verbose --with-xunit --xunit-file=nosetests.xml --with-coverage --cover-xml --cover-xml-file=coverage.xml --logging-config \
#    log_nose.cfg tests/test_*.py
which pytest
pytest --version
ls -l ./pbcommand/
pytest -v --durations=12 --junitxml=nosetests.xml --cov=./pbcommand --cov-report=xml:coverage.xml tests/test_*.py
#pytest -v --durations=12 --junitxml=nosetests.xml tests/test_*.py
sed -i -e 's@filename="@filename="./@g' coverage.xml
which pylint
pylint --version
make run-pylint run-pep8

##########################################################################
# Try python3!
# (This could be done in parallel in Bamboo, for speed.)
# (We do not have py3 wheels in our WHEELHOUSE yet, so we will use PyPI.)
module unload python
module load python/3

# Delete avro.
# avro-python3 would work, but avro must be deleted no matter way.
# https://stackoverflow.com/questions/40732419/how-to-read-avro-files-in-python-3-5-2
rm -rf ${PYTHONUSERBASE}/lib/python3*/site-packages/avro*

#pip3 install --user -r  REQUIREMENTS.txt
pip3 install --user requests iso8601 pytz future
pip3 install --user -e ./
#pip3 install --user -r  REQUIREMENTS_TEST.txt
pip3 install --user nose xmlbuilder
pip3 install --user --upgrade pytest
# --upgrade b/c bin/pytest was python2
#pip3 install --user pbtestdata pbcore
pytest -v --durations=12 tests/test_*.py
##########################################################################

bash bamboo_wheel.sh
