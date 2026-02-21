#!/bin/bash
export MQ_INSTALLATION_PATH=/home/daddy/apps/docker/docker_apps/ibmmq/MQClient
export LD_LIBRARY_PATH=/home/daddy/apps/docker/docker_apps/ibmmq/MQClient/lib64:$LD_LIBRARY_PATH

env_file=$1
req_file=$2
bin_file=$3

if [[ "$bin_file" == *"ibmmq"* ]]; then
    echo "Using MQ Python 3.11 environment"
    source /home/daddy/apps/util/python/mqenv/bin/activate
else
    echo "Using default Python environment"
    source $PYTHON_VENV_DIR/bin/activate
fi

#install requirements
pip install -r $req_file

set -a
source $env_file
set +a

python3 $bin_file
