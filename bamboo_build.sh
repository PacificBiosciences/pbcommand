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
which py.test
py.test --version
ls -l ./pbcommand/
py.test -v --durations=12 --junitxml=nosetests.xml --cov=./pbcommand --cov-report=xml:coverage.xml tests/test_*.py
#py.test -v --durations=12 --junitxml=nosetests.xml tests/test_*.py
sed -i -e 's@filename="@filename="./@g' coverage.xml
which pylint
pylint --version
make run-pylint run-pep8
