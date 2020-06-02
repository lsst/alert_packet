#!/bin/bash

set -e

cd ../

TEMP_VENV_DIR=$(mktemp -d)
virtualenv $TEMP_VENV_DIR
source $TEMP_VENV_DIR/bin/activate

TEMP_DIST_DIR=$(mktemp -d)
python ../setup.py bdist_wheel --dist-dir=$TEMP_DIST_DIR

pip install $TEMP_DIST_DIR/*
validateAvroRoundTrip.py
rm -rf $TEMP_DIST_DIR

deactivate
rm -rf $TEMP_VENV_DIR
