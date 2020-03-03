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


class SchemaValidityTestCase(unittest.TestCase):
    def setUp(self):
        self.registry = SchemaRegistry.from_filesystem()

    def test_schema_validity(self):
        # Assumes that schemas have example data stored as
        # "sample_data/alert.json". If that file doesn't exist, this test is
        # skipped.
        def load_sample_alert(schema_root, version, name="alert.json"):
            try:
                with open(os.path.join(schema_root, *version.split("."),
                                       "sample_data", name), "r") as f:
                    result = json.load(f)
            except FileNotFoundError:
                result = None
            return result

        for version in self.registry.known_versions:
            data = load_sample_alert(get_schema_root(), version)
            if data:
                self.registry.get_by_version(version).validate(data)
