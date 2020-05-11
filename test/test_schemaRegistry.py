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

import json
import os
import unittest
from tempfile import TemporaryDirectory

from lsst.alert.packet import SchemaRegistry

def write_schema(root_dir, filename, version_major, version_minor):
    """Write a simple schema to a filesystem location based on its version.
    """
    # Generate a new schema for each version to avoid ID collisions.
    schema = {
        "name": "example",
        "namespace": "lsst",
        "type": "record",
        "fields": [
            {"name": "field%s%s" % (version_major, version_minor),
             "type": "int"}
        ]
    }
    target_dir = os.path.join(root_dir, version_major, version_minor)
    os.makedirs(target_dir)
    with open(os.path.join(target_dir, filename), "w") as f:
        json.dump(schema, f)

def create_filesystem_hierarchy(root_dir, root_file="lsst.example.avsc"):
    """Create a simple schema hierarchy on the filesystem.
    """
    write_schema(root_dir, root_file, "1", "0")
    write_schema(root_dir, root_file, "2", "0")
    write_schema(root_dir, root_file, "2", "1")

class FromFilesystemTestCase(unittest.TestCase):
    """Demonstrate that the SchemaRegistry can work with on-disk data.
    """

    def test_from_filesystem(self):
        """Check priming a registry based on a simple filesystem hierarchy.
        """
        versions = ("1.0", "2.0", "2.1")

        with TemporaryDirectory() as tempdir:
            create_filesystem_hierarchy(tempdir)
            registry = SchemaRegistry.from_filesystem(tempdir,
                                                      schema_root="lsst.example")

        self.assertEqual(len(registry.known_versions), 3)
        for version in versions:
            self.assertIn(version, registry.known_versions)

        for version in versions:
            registry.get_by_version(version)
        self.assertRaises(KeyError, registry.get_by_version, "2.2")
