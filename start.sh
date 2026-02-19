#!/bin/bash

env_file=$1
req_file=$2
bin_file=$3

#using python virtual env
source $PYTHON_VENV_DIR/bin/activate

#install requirements
pip install -r $req_file

set -a
source $env_file
set +a

python3 $bin_file
