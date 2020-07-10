#!/usr/bin/env python
#
# This file is part of alert_packet.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Demonstrate round-tripping of alert data through Avro serialization.
"""

# Arguably, this should be a test, rather than an executable.

import argparse
import filecmp
import json
import os
import sys
import tempfile

import lsst.alert.packet

# The default filename of per-schema sample alert data.
SAMPLE_FILENAME = "alert.json"


def schema_filename(major_version, minor_version):
    return f"lsst.v{major_version}_{minor_version}.alert.avsc"


def check_file_round_trip(baseline, received_data):
    """Assert that the contents of baseline is equal to received_data.

    Parameters
    ----------
    baseline : `str`
        The full path to a file on disk.
    received_data : `bytes`
        Raw bytes.
    """
    with tempfile.TemporaryDirectory() as tempdir:
        filename = os.path.join(tempdir, "output_file")
        with open(filename, "wb") as f:
            f.write(received_data)
        assert filecmp.cmp(baseline, filename, shallow=False)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--schema-version', type=str,
                        help='Schema version to test (“latest” or MAJOR.MINOR)', default="latest")
    parser.add_argument('--input-data', type=str,
                        help='Path to a file containing schema-compliant JSON Data to serialize',
                        default=None)
    parser.add_argument('--cutout-difference', type=str,
                        help='File for difference image postage stamp')
    parser.add_argument('--cutout-template', type=str,
                        help='File for template image postage stamp')
    parser.add_argument('--print', action="store_true",
                        help='Pretty-print alert contents')

    return parser.parse_args()

def main():
    args = parse_args()
    if args.schema_version == "latest":
        schema_major, schema_minor = lsst.alert.packet.get_latest_schema_version()
    else:
        schema_major, schema_minor = args.schema_version.split(".")
    schema_root = lsst.alert.packet.get_schema_path(schema_major, schema_minor)

    alert_schema = lsst.alert.packet.Schema.from_file(
        os.path.join(schema_root,
                     schema_filename(schema_major, schema_minor)),
    )
    if args.input_data:
        input_data = args.input_data
    else:
        input_data = os.path.join(schema_root, "sample_data", SAMPLE_FILENAME)
    with open(input_data) as f:
        json_data = json.load(f)

    # Load difference stamp if included
    stamp_size = 0
    if args.cutout_difference is not None:
        cutout_difference = lsst.alert.packet.load_stamp(args.cutout_difference)
        stamp_size += len(cutout_difference['stampData'])
        json_data['cutoutDifference'] = cutout_difference

    # Load template stamp if included
    if args.cutout_template is not None:
        cutout_template = lsst.alert.packet.load_stamp(args.cutout_template)
        stamp_size += len(cutout_template['stampData'])
        json_data['cutoutTemplate'] = cutout_template

    # Demonstrate round-trip through Avro serialization
    avro_bytes = alert_schema.serialize(json_data)
    message = alert_schema.deserialize(avro_bytes)

    # Check that postage stamps were preserved through (de)serialization
    if args.cutout_difference:
        check_file_round_trip(args.cutout_difference,
                              message.pop('cutoutDifference')['stampData'])
    if args.cutout_template:
        check_file_round_trip(args.cutout_template,
                              message.pop('cutoutTemplate')['stampData'])

    message_size = len(json.dumps(message).encode('utf-8'))
    print("Size in bytes of JSON-encoded message (excl. stamps): %d" % (message_size,))
    print("Size in bytes of stamps:                              %d" % (stamp_size,))
    print("TOTAL:                                                %d" % (stamp_size + message_size,))
    print("Size in bytes of Avro-encoded message (incl. stamps): %d" % (len(avro_bytes),))
    print("DIFFERENCE:                                           %d" % (stamp_size + message_size
                                                                        - len(avro_bytes),))

    # Pretty-print the received message.
    # Note that the that this won't be identitical to the input because:
    #
    # - Optional fields that were omitted in the input are now present, but
    #   populated with nulls;
    # - Precision has been lost on the floats.
    if args.print:
        print(json.dumps(message, sort_keys=True, indent=4))


if __name__ == "__main__":
    main()
