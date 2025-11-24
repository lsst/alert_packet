#!/bin/bash

# Test that the project can be build as a wheel, and that the wheel can be
# installed, and that validateAvroRoundTrip exits cleanly after installing the
# wheel.
set -e

REPO_ROOT=$(git rev-parse --show-toplevel)

TEMP_VENV_DIR=$(mktemp -d)
virtualenv $TEMP_VENV_DIR
source $TEMP_VENV_DIR/bin/activate

# Make sure we have a recent pip and the build frontend
python -m pip install --upgrade pip
python -m pip install build

TEMP_DIST_DIR=$(mktemp -d)

# Build a wheel from pyproject.toml
cd "$REPO_ROOT"
python -m build --wheel --outdir "$TEMP_DIST_DIR"

# Install the built wheel into the temp venv
python -m pip install "$TEMP_DIST_DIR"/*.whl

# Run the check
validateAvroRoundTrip

# Cleanup
rm -rf "$TEMP_DIST_DIR"
deactivate
rm -rf "$TEMP_VENV_DIR"
