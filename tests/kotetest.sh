#!/bin/sh

# exit when any command fails
set -e

git clone https://github.com/kotekan/kotekan.git --branch rn/comettest --single-branch
cd kotekan/build
cmake -DBOOST_TESTS=ON ..
make -j 4 dataset_broker_producer dataset_broker_producer2 dataset_broker_consumer
cd ../tests
export PYTHONPATH=../python:$PYTHONPATH
pytest -s test_dataset_broker.py