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

import os.path
import unittest

import fastavro
import json

from lsst.alert.packet import get_schema_root, Schema, get_path_to_latest_schema

class SchemaRootTestCase(unittest.TestCase):
    """Test for get_schema_root().
    """

    def test_get_schema_root(self):
        with get_schema_root() as schema_root:
            self.assertTrue(os.path.isdir(schema_root))


class PathLatestSchemTestCase(unittest.TestCase):
    """Test for get_path_to_latest_schema().
    """

    def test_path_latest_schema(self):
        with get_path_to_latest_schema() as schema_path:
            self.assertTrue(os.path.isfile(schema_path))


class ResolveTestCase(unittest.TestCase):
    """Test for schema resolution.
    """
    def test_recursive_resolve(self):
        """Check that resolution of nested schemas gives the expected result.
        """
        # Definition of schemas in terms of each other.
        named_schemas = {}
        sub_sub_schema = fastavro.parse_schema({
            "name": "subsub",
            "namespace": "lsst",
            "type": "record",
            "fields": [
                {"name": "sub_sub_field", "type": "string"}
            ]
        }, named_schemas)

        sub_schema = fastavro.parse_schema({
            "name": "sub",
            "namespace": "lsst",
            "type": "record",
            "fields": [
                {"name": "sub_field", "type": "lsst.subsub"},
                {"name": "second_sub_field", "type": "lsst.subsub"}
            ]
        }, named_schemas)

        top_schema = fastavro.parse_schema({
            "name": "top",
            "namespace": "lsst",
            "type": "record",
            "fields": [
                {"name": "top_field", "type": "lsst.sub"},
                {"name": "boring_field", "type": "int"}
            ]
        }, named_schemas)

        # Derived by substituting the above into each other by hand.
        model_resolved_schema = {
            "type": "record",
            "name": "lsst.top",
            "fields": [
                {
                    "name": "top_field",
                    "type": {
                        "type": "record",
                        "name": "lsst.sub",
                        "fields": [
                            {
                                "name": "sub_field",
                                "type": {
                                    "type": "record",
                                    "name": "lsst.subsub",
                                    "fields": [
                                        {
                                            "name": "sub_sub_field",
                                            "type": "string"
                                        }
                                    ]
                                }
                            },
                            {
                                "name": "second_sub_field",
                                "type": {
                                    "type": "record",
                                    "name": "lsst.subsub",
                                    "fields": [
                                        {
                                            "name": "sub_sub_field",
                                            "type": "string"
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                },
                {
                    "name": "boring_field",
                    "type": "int"
                }
            ]
        }

        resolved_schema = Schema(top_schema).definition
        partially_resolved_schema = fastavro.schema.expand_schema(resolved_schema)
        fully_resolved_schema = fastavro.schema.expand_schema(partially_resolved_schema)

        fastavro_keys = list(fully_resolved_schema.keys())
        for key in fastavro_keys:
            if '__' in key and '__len__' not in key:
                fully_resolved_schema.pop(key)

        self.assertEqual(fully_resolved_schema, model_resolved_schema)
