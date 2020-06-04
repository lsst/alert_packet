#!/bin/bash

# Test that the project can be build as a wheel, and that the wheel can be
# installed, and that validateAvroRoundTrip exits cleanly after installing the
# wheel.
set -e

REPO_ROOT=$(git rev-parse --show-toplevel)

TEMP_VENV_DIR=$(mktemp -d)
virtualenv $TEMP_VENV_DIR
source $TEMP_VENV_DIR/bin/activate

TEMP_DIST_DIR=$(mktemp -d)
TEMP_BUILD_DIR=$(mktemp -d)
python $REPO_ROOT/setup.py bdist_wheel --dist-dir=$TEMP_DIST_DIR --bdist-dir=$TEMP_BUILD_DIR

pip install $TEMP_DIST_DIR/*
validateAvroRoundTrip.py
rm -rf $TEMP_DIST_DIR
rm -rf $TEMP_BUILD_DIR

deactivate
rm -rf $TEMP_VENV_DIR
