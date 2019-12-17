#!/bin/sh

# exit when any command fails
set -e

git clone https://github.com/kotekan/kotekan.git --branch develop --single-branch
cd kotekan/build
cmake -DBOOST_TESTS=ON ..
make dataset_broker_producer dataset_broker_producer2 dataset_broker_consumer

more /proc/cpuinfo | grep flags

cd ../tests
export PYTHONPATH=../python:$PYTHONPATH
pytest -x test_dataset_broker.py
