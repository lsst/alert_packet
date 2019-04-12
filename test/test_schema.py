import unittest

import fastavro
import json

from lsst.alert.packet import Schema

class ResolveTestCase(unittest.TestCase):
    """Test for schema resolution.
    """

    def test_recursive_resolve(self):
        """Check that resolution of nested schemas gives the expected result.
        """
        # Definition of schemas in terms of each other.
        sub_sub_schema = fastavro.parse_schema({
            "name": "subsub",
            "namespace": "lsst",
            "type": "record",
            "fields": [
                {"name": "sub_sub_field", "type": "string"}
            ]
        })

        sub_schema = fastavro.parse_schema({
            "name": "sub",
            "namespace": "lsst",
            "type": "record",
            "fields": [
                {"name": "sub_field", "type": "lsst.subsub"}
            ]
        })

        top_schema = fastavro.parse_schema({
            "name": "top",
            "namespace": "lsst",
            "type": "record",
            "fields": [
                {"name": "top_field", "type": "lsst.sub"},
                {"name": "boring_field", "type": "int"}
            ]
        })

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

        resolved_schema = Schema.resolve(top_schema)
        self.assertEqual(resolved_schema, model_resolved_schema)
