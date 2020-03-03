# This file is part of sample-avro-alert.
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

import os.path
import unittest

import json

from lsst.alert.packet import get_schema_root, SchemaRegistry


def path_to_sample_data(schema_root, version, filename):
    return os.path.join(schema_root, *version.split("."),
                        "sample_data", filename)


class SchemaValidityTestCase(unittest.TestCase):
    def setUp(self):
        self.registry = SchemaRegistry.from_filesystem()

    def test_example_json(self):
        for version in self.registry.known_versions:
            path = path_to_sample_data(get_schema_root(), version, "alert.json")
            if os.path.exists(path):
                with open(path, "r") as f:
                    data = json.load(f)
                self.registry.get_by_version(version).validate(data)

    def test_example_avro(self):
        bad_versions = ("2.0",)  # This data is known not to parse.
        for version in self.registry.known_versions:
            if version in bad_versions:
                continue
            path = path_to_sample_data(get_schema_root(), version,
                                       "fakeAlert.avro")
            if os.path.exists(path):
                with open(path, "rb") as f:
                    self.registry.get_by_version(version).retrieve_alerts(f)
