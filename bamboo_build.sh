#!/bin/bash

type module >& /dev/null || . /mnt/software/Modules/current/init/bash
module purge
module load gcc
module load ccache
module load python/3
module load htslib  # since pysam was built against this
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

# Delete avro.
# avro-python3 would work, but avro must be deleted no matter way.
# https://stackoverflow.com/questions/40732419/how-to-read-avro-files-in-python-3-5-2
rm -rf ${PYTHONUSERBASE}/lib/python3*/site-packages/avro*

$PIP install urllib3==1.21.1
$PIP install requests==2.29.0
pip install --user --no-index --find-link "${WHEELHOUSE}" --no-compile -e '.[test]'
make test
##########################################################################

bash bamboo_wheel.sh
